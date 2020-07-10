using System;
using System.Linq;
using LazyProcessorProject;
using Xunit;

namespace Tests
{
    public class LazyProcessorTests
    {
        private readonly Func<int[], int[]> _getBatchResultFunc = (int[] v) => v.Select(x => x * x).ToArray();

        [Fact]
        public void DefaultTest()
        {
            var values = Enumerable.Range(0, 10000);

            var expectedResult = _getBatchResultFunc(values.ToArray());

            var lazyProcessor = new LazyProcessor();
            var actualResult = lazyProcessor
                .ProcessInBatches(values, _getBatchResultFunc)
                .ToArray();

            Assert.Equal(expectedResult.Length, actualResult.Count());

            Array.Sort(actualResult);

            Assert.Equal(expectedResult, actualResult);
        }

        [Fact]
        public void TestWithBatchSize1()
        {
            var values = Enumerable.Range(0, 10000);
            var expectedResult = _getBatchResultFunc(values.ToArray());

            var lazyProcessor = new LazyProcessor();
            var actualResult = lazyProcessor
                .ProcessInBatches(values, _getBatchResultFunc, batchSize: 1)
                .ToArray();

            Assert.Equal(expectedResult.Length, actualResult.Count());

            Array.Sort(actualResult);

            Assert.Equal(expectedResult, actualResult);
        }

        [Fact]
        public void TestWithDegreeOfParallelism1()
        {
            var values = Enumerable.Range(0, 10000);
            var expectedResult = _getBatchResultFunc(values.ToArray());

            var lazyProcessor = new LazyProcessor();
            var actualResult = lazyProcessor
                .ProcessInBatches(values, _getBatchResultFunc, maxDegreeOfParallelism: 1)
                .ToArray();

            Assert.Equal(expectedResult.Length, actualResult.Count());

            Array.Sort(actualResult);

            Assert.Equal(expectedResult, actualResult);
        }

        [Fact]
        public void TestWithSmallInput()
        {
            var values = Enumerable.Range(0, 3);
            var expectedResult = _getBatchResultFunc(values.ToArray());

            var lazyProcessor = new LazyProcessor();
            var actualResult = lazyProcessor
                .ProcessInBatches(values, _getBatchResultFunc)
                .ToArray();

            Assert.Equal(expectedResult.Length, actualResult.Count());

            Array.Sort(actualResult);

            Assert.Equal(expectedResult, actualResult);
        }

        [Fact]
        public void TestWithLargeInput()
        {
            var values = Enumerable.Range(0, 10000000);
            var expectedResult = _getBatchResultFunc(values.ToArray());

            var lazyProcessor = new LazyProcessor();
            var actualResult = lazyProcessor
                .ProcessInBatches(values, _getBatchResultFunc)
                .ToArray();

            Assert.Equal(expectedResult.Length, actualResult.Count());

            Array.Sort(expectedResult); //sort expected result because of integer overflow
            Array.Sort(actualResult);

            Assert.Equal(expectedResult, actualResult);
        }

        [Fact]
        public void TestWithEmptyInput()
        {
            var values = Enumerable.Empty<int>();

            var lazyProcessor = new LazyProcessor();
            var actualResult = lazyProcessor
                .ProcessInBatches(values, _getBatchResultFunc)
                .ToArray();

            Assert.True(actualResult.Count() == 0);
        }

        [Fact]
        public void IncorrectBatchSizeThrowsException()
        {
            var values = Enumerable.Empty<int>();

            var lazyProcessor = new LazyProcessor();

            Assert.Throws<ArgumentException>(() => lazyProcessor
                .ProcessInBatches(values, _getBatchResultFunc, batchSize: 0)
                .ToArray());
        }

        [Fact]
        public void IncorrectMaxDegreeOfParallelismThrowsException()
        {
            var values = Enumerable.Empty<int>();

            var lazyProcessor = new LazyProcessor();

            Assert.Throws<ArgumentException>(() => lazyProcessor
                .ProcessInBatches(values, _getBatchResultFunc, maxDegreeOfParallelism: 0)
                .ToArray());
        }
    }
}