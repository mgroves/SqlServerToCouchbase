using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace SqlServerToCouchbase.Console
{
    class Program
    {
        static async Task Main(string[] args)
        {
            var config = new ConfigurationBuilder()
                .SetBasePath(AppDomain.CurrentDomain.BaseDirectory)
                .AddJsonFile("appsettings.json")
                .AddEnvironmentVariables()
                .Build();

            var tableNameCollectionMapping = config.GetSection("TableNameToCollectionMapping")
                .GetChildren()
                .ToDictionary(x => x.Key, x => x.Value);

            var migrateConfig = new SqlToCbConfig
            {
                SourceSqlConnectionString = config.GetValue<string>("SqlServer:ConnectionString"),
                TargetBucket = config.GetValue<string>("CouchbaseServer:Bucket"),
                TargetBucketRamQuotaMB = config.GetValue<int>("CouchbaseServer:TargetBucketRamQuotaMB"),
                TargetConnectionString = config.GetValue<string>("CouchbaseServer:ConnectionString"),
                TargetUsername = config.GetValue<string>("CouchbaseServer:Username"),
                TargetPassword = config.GetValue<string>("CouchbaseServer:Password"),
                TableNameToCollectionMapping = tableNameCollectionMapping,
                UseSchemaForScope = config.GetValue<bool>("UseSchemaForScope"),
                UseDefaultScopeForDboSchema = config.GetValue<bool>("UseDefaultScopeForDboSchema"),
                DefaultPasswordForUsers = config.GetValue<string>("CouchbaseServer:DefaultUserPassword")
            };

            // setup DI for logging/HTTP
            var serviceProvider = new ServiceCollection()
                .AddLogging(builder => builder
                    .AddConsole()
                    .AddFilter(level => level >= LogLevel.Information)
                )
                .AddSingleton<SqlToCbConfig>(migrateConfig)
                .AddTransient<SqlToCb>()
                .BuildServiceProvider();

            // since this is a console app, just use locator to get a SqlToCb instance
            var convert = serviceProvider.GetService<SqlToCb>();

            // double check
            if(convert == null)
                throw new Exception("Something went wrong instantiating a SqlToCb object");

            try
            {
                // instructions for migration run
                var shouldValidateNames = config.GetValue<bool?>("Instructions:ValidateNames") ?? false;
                var shouldCreateBucket = config.GetValue<bool?>("Instructions:CreateBucket") ?? false;
                var shouldCreateCollections = config.GetValue<bool?>("Instructions:CreateCollections") ?? false;
                var shouldCreateUsers = config.GetValue<bool?>("Instructions:CreateUsers") ?? false;
                var shouldCreateIndexes = config.GetValue<bool?>("Instructions:CreateIndexes") ?? false;
                var shouldSampleIndexes = config.GetValue<bool?>("Sampling:SampleIndexes") ?? false;
                var shouldCreateData = config.GetValue<bool?>("Instructions:CreateData") ?? false;
                var shouldSampleData = config.GetValue<bool?>("Sampling:SampleData") ?? false;

                var pipelines = new SqlPipelines();
                pipelines.Add(new ModifiedDateSqlFilter(new DateTime(2014, 05, 27), "Person", "Address"));
                pipelines.Add(new ScrambleSensitivePipeline("Person", "Person", "FirstName", "LastName"));

                var sw = new Stopwatch();
                sw.Start();

                await convert.ConnectAsync();

                if (shouldValidateNames)
                    await convert.MigrateAsync(validateNames: true);

                if(shouldCreateBucket)
                    await convert.MigrateAsync(createBucket: true);
                
                if(shouldCreateCollections)
                    await convert.MigrateAsync(createCollections: true);

                if(shouldCreateUsers)
                    await convert.MigrateAsync(createUsers: true);

                if(shouldCreateIndexes)
                    await convert.MigrateAsync(createIndexes: true, sampleForDemo: shouldSampleIndexes);

                if(shouldCreateData)
                    await convert.MigrateAsync(copyData: true, sampleForDemo: shouldSampleData, pipelines: pipelines);

                sw.Stop();
                System.Console.WriteLine("***************************");
                System.Console.WriteLine($"Time elapsed: {sw.Elapsed}");
                System.Console.WriteLine("***************************");
            }
            finally
            {
                convert.Dispose();
            }
        }
    }
}
