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
        }

        public IEnumerable<TResult> Run()
        {
            if (!_sourceValues.Any())
                yield break;

            Task.Run(() => ScheduleBatchTasks());

            foreach (var item in _resultBuffer.GetConsumingEnumerable())
                yield return item;
        }

        private void ScheduleBatchTasks()
        {
            Parallel.ForEach(
                SplitInputToBlocks(),
                new ParallelOptions { MaxDegreeOfParallelism = _maxDegreeOfParallelism },
                RunBatchTask);

            _resultBuffer.CompleteAdding();
        }

        private IEnumerable<TValue[]> SplitInputToBlocks()
        {
            var taskInput = new List<TValue>();
            foreach (var v in _sourceValues)
            {
                taskInput.Add(v);

                if (taskInput.Count == _batchSize)
                {
                    yield return taskInput.ToArray();
                    taskInput.Clear();
                }
            }

            if (taskInput.Count > 0)
            {
                yield return taskInput.ToArray();
            }
        }

        private void RunBatchTask(TValue[] inputBlock)
        {
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

        private bool _allInputProcessed = false;
        private readonly IEnumerable<TValue> _sourceValues;
        private readonly Func<TValue[], TResult[]> _getBatchResultFunc;
        private readonly int _batchSize;
        private readonly int _maxDegreeOfParallelism;
        private const int _resultBufferBoundedCapacity = 3;
        private readonly BlockingCollection<TResult> _resultBuffer = new BlockingCollection<TResult>(_resultBufferBoundedCapacity);
    }
}