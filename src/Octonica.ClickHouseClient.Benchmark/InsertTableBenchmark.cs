
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Engines;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Octonica.ClickHouseClient.Benchmark
{
    [MemoryDiagnoser]
    [MarkdownExporter, RPlotExporter]
    
    [SimpleJob(RunStrategy.ColdStart, 
        launchCount: 1,
        warmupCount: 1, 
        targetCount: 20,
        id: "Insert into table")]
    public class InsertTableBenchmark: ClickHouseBaseConnection
	{

        private readonly string TableName = "insert_benchmark";
        private ClickHouseConnection connection;

        [Params(10000, 100000, 1000000)]
        public int valuesCount { get; set; }

        private IEnumerable<int> idEnumerable { get; set; }
        private IEnumerable<string> strEnumerable { get; set; }

        [GlobalSetup]
        public void GlobalSetup()
        {
            idEnumerable = Enumerable.Range(0, valuesCount);
            strEnumerable = idEnumerable.Select(x => x.ToString());
        }

        [IterationSetup]
        public void IterationSetup()
        {
            connection = OpenConnection();
            IterationCleanup();

            var cmd = connection.CreateCommand($"CREATE TABLE {TableName}(id Int32, str Nullable(String)) ENGINE=Memory");
            cmd.ExecuteNonQuery();

        }

        [IterationCleanup]
        public void IterationCleanup()
        {
            var cmd = connection.CreateCommand($"DROP TABLE IF EXISTS {TableName}");
            cmd.ExecuteNonQuery();
        }


        [Benchmark]
        public void InsertValues() {
                
            using (var writer = connection.CreateColumnWriter($"INSERT INTO {TableName}(id, str) VALUES"))
            {
                var writeObject = new object[] { idEnumerable, strEnumerable };
                writer.WriteTable(writeObject, valuesCount);
                writer.EndWrite();
            }
        }

    }
}
