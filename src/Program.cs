using System;
using System.Linq;

namespace LazyProcessorProject
{
    class Program
    {
        static void Main(string[] args)
        {
            var lazyProcessor = new LazyProcessor();
            var values = Enumerable.Range(0, 1000);
            var result = lazyProcessor.ProcessInBatches(
                values,
                (int[] v) => v.Select(x => x * x).ToArray(), 10, 5);

            foreach (var item in result)
            {
                Console.WriteLine(item);
            }
        }
    }
}
