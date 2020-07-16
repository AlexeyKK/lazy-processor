using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace LazyProcessorProject
{
    public class LazyProcessor
    {
        public IEnumerable<TResult> ProcessInBatches<TValue, TResult>(
               IEnumerable<TValue> sourceValues,
               Func<TValue[], TResult[]> getBatchResultFunc,
               int batchSize = 100,
               int maxDegreeOfParallelism = 10)
        {
            var lpu = new LazyProcessorUnit<TValue, TResult>(
                sourceValues,
                getBatchResultFunc,
                batchSize,
                maxDegreeOfParallelism);
            return lpu.Run();
        }
    }

    public class LazyProcessorUnit<TValue, TResult>
    {
        public LazyProcessorUnit(
            IEnumerable<TValue> sourceValues,
            Func<TValue[], TResult[]> getBatchResultFunc,
            int batchSize = 100,
            int maxDegreeOfParallelism = 10)
        {
            ThrowIfNull(nameof(sourceValues), sourceValues);
            ThrowIfNull(nameof(getBatchResultFunc), getBatchResultFunc);
            if (batchSize <= 0)
                throw new ArgumentException($"{nameof(batchSize)} must be greater than zero");
            if (maxDegreeOfParallelism <= 0)
                throw new ArgumentException($"{nameof(maxDegreeOfParallelism)} must be greater than zero");

            _sourceValues = sourceValues;
            _getBatchResultFunc = getBatchResultFunc;
            _batchSize = batchSize;
            _maxDegreeOfParallelism = maxDegreeOfParallelism;
            _sourceValuesEnumerator = sourceValues.GetEnumerator();
        }

        public IEnumerable<TResult> Run()
        {
            if (!_sourceValues.Any())
                yield break;

            var schedulingTask = ScheduleBatchTasks();

            foreach (var item in _resultBuffer.GetConsumingEnumerable())
                yield return item;

            schedulingTask.Wait();
        }

        private Task ScheduleBatchTasks()
        {
            var tasks = new Task[_maxDegreeOfParallelism];
            for (int i = 0; i < _maxDegreeOfParallelism; i++)
            {
                tasks[i] = Task.Run(() => {
                    TValue[] batch;
                    while(ReadNextBatch(out batch))
                    {
                        RunBatchTask(batch);
                    }
                });
            }
            return Task.WhenAll(tasks)
                .ContinueWith(_ => _resultBuffer.CompleteAdding());
        }

        private bool ReadNextBatch(out TValue[] batch)
        {
            lock(_nextBatchLockObj)
            {
                var count = 0;
                var batchRead = false;

                while(count < _batchSize && _sourceValuesEnumerator.MoveNext())
                {
                    _batchInputBlock.Add(_sourceValuesEnumerator.Current);
                    count++;
                    batchRead = true;
                }
                
                batch = _batchInputBlock.ToArray();
                _batchInputBlock.Clear();
                return batchRead;
            }
        }

        private void RunBatchTask(TValue[] inputBlock)
        {
            if(inputBlock?.Length == 0) return;

            Console.WriteLine($"Start work on thread {Thread.CurrentThread.ManagedThreadId}");
            var resultBlock = _getBatchResultFunc(inputBlock);

            if (resultBlock != null)
            {
                foreach (var item in resultBlock)
                    _resultBuffer.Add(item);
            }
        }

        private void ThrowIfNull(string parameterName, object parameter)
        {
            if (parameter == null)
                throw new ArgumentNullException($"{parameterName} can not be null");
        }

        private readonly IEnumerable<TValue> _sourceValues;
        private readonly IEnumerator<TValue> _sourceValuesEnumerator;
        private readonly Func<TValue[], TResult[]> _getBatchResultFunc;
        private readonly int _batchSize;
        private readonly int _maxDegreeOfParallelism;
        private readonly List<TValue> _batchInputBlock = new List<TValue>();
        private readonly object _nextBatchLockObj = new object();
        private readonly BlockingCollection<TResult> _resultBuffer = new BlockingCollection<TResult>();
    }
}