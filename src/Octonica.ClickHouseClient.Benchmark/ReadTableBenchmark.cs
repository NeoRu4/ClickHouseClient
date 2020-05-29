
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
        id: "Read data from table")]
    public class ReadTableBenchmark: ClickHouseBaseConnection
	{

        private readonly string TableName = "insert_benchmark";
        private ClickHouseConnection connection;

        [Params(10000, 100000, 1000000)]
        public int valuesCount { get; set; }

        private IEnumerable<int> idEnumerable { get; set; }
        private IEnumerable<string> strEnumerable { get; set; }

        public ReadTableBenchmark()
        {
            connection = OpenConnection();
        }

        [GlobalSetup]
        public void GlobalSetup()
        {

            GlobalCleanup();

            var cmd = connection.CreateCommand($"CREATE TABLE {TableName}(id Int32, str Nullable(String)) ENGINE=Memory");
            cmd.ExecuteNonQuery();

            idEnumerable = Enumerable.Range(0, valuesCount);
            strEnumerable = idEnumerable.Select( x => x.ToString());

            //Fill Table
            using (var writer = connection.CreateColumnWriter($"INSERT INTO {TableName}(id, str) VALUES"))
            {
                var writeObject = new object[] { idEnumerable, strEnumerable };
                writer.WriteTable(writeObject, valuesCount);
                writer.EndWrite();
            }
        }

        [GlobalCleanup]
        public void GlobalCleanup()
        {
            var cmd = connection.CreateCommand($"DROP TABLE IF EXISTS {TableName}");
            cmd.ExecuteNonQuery();
        }

        [Benchmark]
        public void ReadValues()
        {
            var cmd = connection.CreateCommand($"SELECT id, str FROM {TableName}");
            int count = 0;

            using (var reader = cmd.ExecuteReader())
            {
                while (reader.Read())
                {
                    var id = reader.GetInt32(0);
                    var str = reader.GetString(1, null);

                    count++;
                }
            }
        }
    }
}
