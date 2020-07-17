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
            _resultBuffer = new BlockingCollection<TResult>(_batchSize * _maxDegreeOfParallelism);
        }

        public IEnumerable<TResult> Run()
        {
            if (!_sourceValues.Any())
                yield break;

            var task = RunBatchTasks();

            foreach (var item in _resultBuffer.GetConsumingEnumerable())
                yield return item;

            task.Wait();
        }

        private Task RunBatchTasks()
        {
            var tasks = new Task[_maxDegreeOfParallelism];
            for (int i = 0; i < _maxDegreeOfParallelism; i++)
            {
                tasks[i] = Task.Run(() =>
                {
                    var batch = new TValue[_batchSize];
                    var actualBatchLength = 0;
                    while (ReadNextBatch(ref batch, out actualBatchLength))
                    {
                        if (actualBatchLength < _batchSize)
                            RunBatchTask(batch.Take(actualBatchLength).ToArray());
                        else
                            RunBatchTask(batch);
                    }
                });
            }
            return Task.WhenAll(tasks)
                .ContinueWith(_ => _resultBuffer.CompleteAdding());
        }

        private bool ReadNextBatch(ref TValue[] batch, out int actualBatchLength)
        {
            lock (_readNextBatchLockObj)
            {
                actualBatchLength = 0;
                var batchRead = false;

                while (actualBatchLength < _batchSize && _sourceValuesEnumerator.MoveNext())
                {
                    batch[actualBatchLength] = _sourceValuesEnumerator.Current;
                    actualBatchLength++;
                    batchRead = true;
                }

                return batchRead;
            }
        }

        private void RunBatchTask(TValue[] inputBlock)
        {
            if (inputBlock?.Length == 0) return;

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
        private readonly object _readNextBatchLockObj = new object();
        private readonly BlockingCollection<TResult> _resultBuffer;
    }
}