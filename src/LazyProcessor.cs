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
            if(batchSize <= 0)
                throw new ArgumentException($"{nameof(batchSize)} must be greater than zero");
            if(maxDegreeOfParallelism <= 0)
                throw new ArgumentException($"{nameof(maxDegreeOfParallelism)} must be greater than zero");

            _sourceValues = sourceValues;
            _getBatchResultFunc = getBatchResultFunc;
            _batchSize = batchSize;
            _maxDegreeOfParallelism = maxDegreeOfParallelism;
        }

        public IEnumerable<TResult> Run()
        {
            if (!_sourceValues.Any())
                yield break;

            Task.Run(() => ScheduleBatchTasks());

            bool resultBufferIsNotComplete = true;
            TResult[] resultBlock;
            while (resultBufferIsNotComplete)
            {
                try
                {
                    resultBlock = _resultBuffer.Take();
                }
                catch (InvalidOperationException)
                {
                    resultBufferIsNotComplete = false;
                    yield break;
                }

                if (resultBlock != null)
                {
                    foreach (var item in resultBlock)
                        yield return item;
                }
            }
        }

        private void ScheduleBatchTasks()
        {
            Parallel.ForEach(
                SplitInputToBlocks(), 
                new ParallelOptions { MaxDegreeOfParallelism = _maxDegreeOfParallelism }, 
                RunBatchTask);
        }

        private IEnumerable<TValue[]> SplitInputToBlocks()
        {
            var taskInput = new List<TValue>();
            foreach (var v in _sourceValues)
            {
                taskInput.Add(v);

                if (taskInput.Count == _batchSize)
                {
                    _numberOfInputBlocksCreated++;
                    yield return taskInput.ToArray();
                    taskInput.Clear();
                }
            }

            if (taskInput.Count > 0)
            {
                _numberOfInputBlocksCreated++;
                yield return taskInput.ToArray();
            }

            _allInputProcessed = true;
        }

        private void RunBatchTask(TValue[] inputBlock)
        {
            var resultBlock = _getBatchResultFunc(inputBlock);

            _resultBuffer.Add(resultBlock);

            Interlocked.Increment(ref _numberOfResultBlocksCreated);
            if (Interlocked.Equals(_numberOfInputBlocksCreated, _numberOfResultBlocksCreated)
                && _allInputProcessed)
                _resultBuffer.CompleteAdding();
        }

        private void ThrowIfNull(string parameterName, object parameter)
        {
            if (parameter == null)
                throw new ArgumentNullException($"{parameterName} can not be null");
        }

        private int _numberOfInputBlocksCreated = 0;
        private int _numberOfResultBlocksCreated = 0;
        private bool _allInputProcessed = false;
        private readonly IEnumerable<TValue> _sourceValues;
        private readonly Func<TValue[], TResult[]> _getBatchResultFunc;
        private readonly int _batchSize;
        private readonly int _maxDegreeOfParallelism;
        private readonly BlockingCollection<TResult[]> _resultBuffer = new BlockingCollection<TResult[]>();
    }
}