using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;
using System.Threading.Tasks;

namespace MapReduceOnFiles
{
  static class Program
  {
    static ParallelQuery<TResult> MapReduce<TSource, TMapped, TKey, TResult>(
                            this ParallelQuery<TSource> source,
                            Func<TSource, IEnumerable<TMapped>> map,
                            Func<TMapped, TKey> keySelector,
                            Func<IGrouping<TKey, TMapped>, IEnumerable<TResult>> reduce)
    {
      //return source.SelectMany(map).GroupBy(keySelector).SelectMany(reduce);
      ParallelQuery<TMapped> mapRes = source.SelectMany(map);
      //List<TMapped> debug1 = mapRes.ToList();

      ParallelQuery<IGrouping<TKey, TMapped>> groupByRes = mapRes.GroupBy(keySelector);
      //List<TKey> debug2 = groupByRes.Select(item => item.Key).ToList();

      ParallelQuery<TResult> res = groupByRes.SelectMany(reduce);
      //List<TResult> debug3 = res.ToList();

      return res;
    }

    static void Main(string[] args)
    {
        string dirPath = @".\data\";
      char[] delimiters = Enumerable.Range(0, 256).Select(i => (char)i).Where(c => Char.IsWhiteSpace(c) || Char.IsPunctuation(c)).ToArray();
      var files = Directory.EnumerateFiles(dirPath, "*.txt").AsParallel();
      
      var counts = files.MapReduce(
        path => File.ReadLines(path, Encoding.GetEncoding(1252)).SelectMany(line => line.ToLower().Split(delimiters)), 
        word => word,
        group => new[] { new KeyValuePair<string, int>(group.Key, group.Count()) });

      // selection
      int charLen = 5;
      int occurNum = 1000;

      Console.WriteLine("Words with at least " + charLen + " characters and that occur at least " + occurNum + " times");
      var ordCounts = counts.Where(item => item.Key.Length >= charLen && item.Value > occurNum).OrderBy(item => item.Key);
      foreach (var item in ordCounts)
      {
        Console.WriteLine(item.Key + ": " + item.Value);
      }

      Console.In.Read();
    }
  }
}
