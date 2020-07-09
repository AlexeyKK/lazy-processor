using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace LazyProcessorProject
{


    class Program
    {
        static void Main(string[] args)
        {
            var lazyProcessor = new LazyProcessor();
            var values = Enumerable.Range(0, 10);
            var result = lazyProcessor.ProcessInBatches(
                values,
                (int[] v) => v.Select(x => x * x).ToArray(), 1);

            foreach (var item in result)
            {
                Console.WriteLine(item);
            }
        }
    }
}
