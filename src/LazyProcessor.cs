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
        private int degreeOfParallelism = 0;

        public IEnumerable<TResult> ProcessInBatches<TValue, TResult>(
               IEnumerable<TValue> sourceValues,
               Func<TValue[], TResult[]> getBatchResultFunc,
               int batchSize = 100,
               int maxDegreeOfParallelism = 10)
        {
            ThrowIfNull(nameof(sourceValues), sourceValues);
            ThrowIfNull(nameof(getBatchResultFunc), getBatchResultFunc);

            using (var lpu = new LazyProcessorUnit<TValue, TResult>(
                sourceValues,
                getBatchResultFunc,
                batchSize,
                maxDegreeOfParallelism))
            {
                return lpu.Run();
            }
        }

        private void ThrowIfNull(string parameterName, object parameter)
        {
            if (parameter == null)
                throw new ArgumentNullException($"{parameterName} can not be null");
        }
    }

    public class LazyProcessorUnit<TValue, TResult> : IDisposable
    {
        public LazyProcessorUnit(
            IEnumerable<TValue> sourceValues,
            Func<TValue[], TResult[]> getBatchResultFunc,
            int batchSize = 100,
            int maxDegreeOfParallelism = 10)
        {
            _sourceValues = sourceValues;
            _getBatchResultFunc = getBatchResultFunc;
            _batchSize = batchSize;
            _maxDegreeOfParallelism = maxDegreeOfParallelism;
        }

        public void Dispose()
        {
        }

        public IEnumerable<TResult> Run()
        {
            Task.Run(() => ScheduleBatchTasks());

            TResult[] resultBlock;
            do
            {
                while (!_resultBuffer.TryTake(out resultBlock, _timeout));

                foreach (var item in resultBlock)
                    yield return item;
                
                _resultBlocksProcessed++;
            }
            while (_resultBlocksProcessed != _resultBlocksCreated);
        }

        private void ScheduleBatchTasks()
        {
            var taskInput = new List<TValue>();
            foreach (var v in _sourceValues)
            {
                taskInput.Add(v);

                if (taskInput.Count == _batchSize)
                {
                    RunBatchTask(taskInput.ToArray());
                    taskInput.Clear();
                }
            }

            if (taskInput.Count > 0)
            {
                RunBatchTask(taskInput.ToArray());
            }
        }

        private void RunBatchTask(TValue[] inputBlock)
        {
            Task.Run(() =>
            {
                var resultBlock = _getBatchResultFunc(inputBlock);
                while (!_resultBuffer.TryAdd(resultBlock, _timeout));
                Interlocked.Increment(ref _resultBlocksCreated);
            });
        }

        private int _resultBlocksCreated = 0;
        private int _resultBlocksProcessed = 0;
        private readonly IEnumerable<TValue> _sourceValues;
        private readonly Func<TValue[], TResult[]> _getBatchResultFunc;
        private readonly int _batchSize;
        private readonly int _maxDegreeOfParallelism;
        // private readonly AutoResetEvent _resultQueueIsFree = new AutoResetEvent(false);
        private const int _timeout = 10;
        private const int _resultQueueBoundedCapacity = 2;
        private readonly BlockingCollection<TResult[]> _resultBuffer = new BlockingCollection<TResult[]>(_resultQueueBoundedCapacity);
    }

    public class BlockingArrayPool<T>
    {
        public BlockingArrayPool(int maxPoolSize, int arraySize)
        {
            _maxPoolSize = maxPoolSize;
            _arraySize = arraySize;
            _pool = new BlockingCollection<T[]>(maxPoolSize);
        }

        public T[] Get()
        {
            lock (lockObject)
            {
                if (_pool.Count == 0 && _poolSize < _maxPoolSize)
                {
                    _poolSize++;
                    _pool.Add(new T[_arraySize]);
                }
                return _pool.Take();
            }
        }

        public void Add(T[] array)
        {
            if (_pool.Count < _maxPoolSize)
                _pool.Add(array);
        }

        private int _poolSize = 0;
        private readonly object lockObject = new object();
        private readonly int _maxPoolSize;
        private readonly int _arraySize;
        private BlockingCollection<T[]> _pool;
    }
}