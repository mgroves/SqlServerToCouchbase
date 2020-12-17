using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Couchbase;
using Couchbase.Core.IO.Serializers;
using Couchbase.Management.Buckets;
using Couchbase.Management.Collections;
using Dapper;
using Dynamitey;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;

namespace SqlServerToCouchbase
{
    public class SqlToCb : IDisposable
    {
        private readonly SqlToCbConfig _config;
        private readonly ILogger _logger;
        private SqlConnection _sqlConnection;
        private ICluster _cluster;
        private IBucket _bucket;
        private Dictionary<string, List<string>> _primaryKeyNames;
        private bool _isValid;

        public SqlToCb(SqlToCbConfig config, ILogger logger)
        {
            _config = config;
            _logger = logger;
            _isValid = false;
        }

        public async Task Connect()
        {
            _logger.LogInformation("Connecting to SQL Server...");
            _sqlConnection = new SqlConnection(_config.SourceSqlConnectionString);
            _logger.LogInformation("Done");

            _logger.LogInformation("Connecting to Couchbase...");
            var serializerSettings = new JsonSerializerSettings();
            serializerSettings.AddSqlConverters();

            var options = new ClusterOptions
            {
                UserName = _config.TargetUsername,
                Password = _config.TargetPassword,
                Serializer = new DefaultSerializer(serializerSettings, serializerSettings)
            };
            _cluster = await Cluster.ConnectAsync(_config.TargetConnectionString, options);
            _logger.LogInformation("Done");
        }

        public void Dispose()
        {
            try
            {
                _logger.LogInformation("Disconnecting from SQL Server...");
                _sqlConnection.Close();
                _sqlConnection.Dispose();
                _logger.LogInformation("Done");
            }
            catch (Exception ex)
            {
                _logger.LogError($"something went wrong: {ex.Message}");
            }

            try
            {
                _logger.LogInformation("Disconnecting from Couchbase...");
                _cluster.Dispose();
                _logger.LogInformation("Done");
            }
            catch (Exception ex)
            {
                _logger.LogError($"something went wrong: {ex.Message}");
            }
        }

        private async Task ValidateNames()
        {
            var tables = (await _sqlConnection.QueryAsync(@"
                select TABLE_SCHEMA, TABLE_NAME
                from INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'")).ToList();
            foreach (var table in tables)
            {
                string tableName = table.TABLE_SCHEMA + '_' + table.TABLE_NAME;
                if (tableName.Length <= 30)
                {
                    _logger.LogInformation($"Name `{tableName}` is fine.");
                    continue;
                }

                var hasMapping = _config.TableNameToCollectionMapping.ContainsKey(tableName);
                if (!hasMapping)
                {
                    _logger.LogError($"Name `{tableName}` is too long and no mapping found. Can't continue.");
                    _isValid = false;
                    return;
                }
            }
            _isValid = true;
        }

        public async Task Migrate(
            bool validateNames = false,
            bool createBucket = false,
            bool createCollections = false,
            bool createIndexes = false,
            bool copyData = false)
        {
            if (validateNames) await ValidateNames();

            var shouldContinue = createBucket || createCollections || createIndexes || copyData;

            if (!shouldContinue)
                return;

            if (!_isValid)
                throw new Exception("Validation on SQL Server names has not been performed. Cannot continue with migration.");

            if (createBucket)
                await CreateBucket();
            else
                await ConnectBucket();

            if (createCollections) await CreateCollections();

            if (createIndexes) await CreateIndexes();

            if (copyData) await CopyData();
        }

        private async Task CreateBucket()
        {
            var bucketManager = _cluster.Buckets;
            var bucketSettings = new BucketSettings
            {
                BucketType = BucketType.Couchbase,
                Name = _config.TargetBucket,
                RamQuotaMB = _config.TargetBucketRamQuotaMB
            };
            _logger.LogInformation($"Creating bucket `{_config.TargetBucket}`...");
            try
            {
                await bucketManager.CreateBucketAsync(bucketSettings);
                _logger.LogInformation("Done");
            }
            catch (BucketExistsException)
            {
                _logger.LogInformation("already exists.");
            }
            _bucket = await _cluster.BucketAsync(_config.TargetBucket);
        }

        private async Task ConnectBucket()
        {
            _bucket = await _cluster.BucketAsync(_config.TargetBucket);
        }

        private async Task CreateCollections()
        {
            var tables = (await _sqlConnection.QueryAsync(@"
                select TABLE_SCHEMA, TABLE_NAME
                from INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'")).ToList();
            var collManager = _bucket.Collections;
            foreach (var table in tables)
            {
                string rawCollectionName = table.TABLE_SCHEMA + '_' + table.TABLE_NAME;
                var collectionName = _config.TableNameToCollectionMapping.ContainsKey(rawCollectionName)
                    ? _config.TableNameToCollectionMapping[rawCollectionName]
                    : rawCollectionName;

                _logger.LogInformation($"Creating collection `{collectionName}`...");
                if (CollectionExists(collectionName))
                {
                    _logger.LogInformation("already exists.");
                    continue;
                }
        
                var spec = new CollectionSpec("_default", collectionName);
                await collManager.CreateCollectionAsync(spec);
                _logger.LogInformation("Done");
            }
        }

        private bool CollectionExists(string collectionName)
        {
            try
            {
                _bucket.Collection(collectionName);
                return true;
            }
            catch (Couchbase.Core.Exceptions.CollectionNotFoundException)
            {
                return false;
            }
        }

        private async Task CreateIndexes()
        {
            _logger.LogInformation("Creating indexes...");
            var indexes = (await _sqlConnection.QueryAsync(@"
                select i.[name] as index_name,
                    substring(column_names, 1, len(column_names)-1) as [columns],
                    schema_name(t.schema_id) + '.' + t.[name] as table_view
                from sys.objects t
                    inner join sys.indexes i
                        on t.object_id = i.object_id
                    cross apply (select col.[name] + ', '
                                    from sys.index_columns ic
                                        inner join sys.columns col
                                            on ic.object_id = col.object_id
                                            and ic.column_id = col.column_id
                                    where ic.object_id = t.object_id
                                        and ic.index_id = i.index_id
                                            order by key_ordinal
                                            for xml path ('') ) D (column_names)
                where t.is_ms_shipped <> 1
                and index_id > 0
                and t.[type] = 'U'
                order by i.[name]
                ")).ToList();
            var indexManager = _cluster.QueryIndexes;
            foreach (var index in indexes)
            {
                string indexName = index.index_name.ToString();


                var collectionName = index.table_view.ToString().Replace(".", "_");
                if (_config.TableNameToCollectionMapping.ContainsKey(collectionName))
                    collectionName = _config.TableNameToCollectionMapping[collectionName];
                _logger.LogInformation($"SQL Server Index: `{indexName}`...");
                string fields = string.Join(",", ((string)index.columns)
                    .Split(',')
                    .Select(f => $"`{f}`"));
                await _cluster.QueryAsync<dynamic>(
                    $"CREATE INDEX `sql_{indexName}` ON `{_config.TargetBucket}`.`_default`.`{collectionName}` ({fields})");
                _logger.LogInformation("Done.");
            }
        }

        private async Task CopyData()
        {
            _primaryKeyNames = new Dictionary<string, List<string>>();

            var tables = (await _sqlConnection.QueryAsync(@"
                select TABLE_SCHEMA, TABLE_NAME
                from INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'")).ToList();
            foreach (var table in tables)
                await CopyDataFromTableToCollection(table.TABLE_SCHEMA, table.TABLE_NAME);
        }

        private async Task CopyDataFromTableToCollection(string tableSchema, string tableName)
        {
            var collectionName = tableSchema + "_" + tableName;
            if (_config.TableNameToCollectionMapping.ContainsKey(collectionName))
                collectionName = _config.TableNameToCollectionMapping[collectionName];

            _logger.LogInformation($"Copying data from `{tableSchema}.{tableName}` table to {collectionName} collection...");
            var counter = 0;

            var collection = _bucket.Collection(collectionName);
            var rows = _sqlConnection.Query($@"
                SELECT *
                FROM [{tableSchema}].[{tableName}]");

            _logger.LogInformation("Writing row(s)...");
            foreach (var row in rows)
            {
                var documentKey = await GetDocumentKeyFromPrimaryKeyValues(row, tableSchema, tableName);
                await collection.UpsertAsync(documentKey, row);
                counter++;
                if ((counter % 1000) == 0)
                    _logger.LogInformation(counter + "...");
            }
            _logger.LogInformation("Done");
        }

        private async Task<string> GetDocumentKeyFromPrimaryKeyValues(dynamic row, string tableSchema, string tableName)
        {
            // check to see if the key name are already cached
            if (!_primaryKeyNames.ContainsKey($"{tableSchema}.{tableName}"))
            {
                var keyNames = (await _sqlConnection.QueryAsync<string>(@"SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                    WHERE TABLE_NAME = @tableName
                    AND TABLE_SCHEMA = @tableSchema
                    AND CONSTRAINT_NAME LIKE 'PK_%'", new { tableName, tableSchema })).ToList();
                _primaryKeyNames.Add($"{tableSchema}.{tableName}", keyNames.ToList());
            }

            var keys = _primaryKeyNames[$"{tableSchema}.{tableName}"];
            var newKey = $"{tableSchema}::{tableName}::" +
                         string.Join("::", keys.Select(k => Dynamic.InvokeGet(row, k)));
            return newKey;
        }
    }

    public class SqlToCbConfig
    {
        public string SourceSqlConnectionString { get; set; }
        public string TargetConnectionString { get; set; }
        public string TargetUsername { get; set; }
        public string TargetPassword { get; set; }
        public string TargetBucket { get; set; }
        public int TargetBucketRamQuotaMB { get; set; }
        public IDictionary<string,string> TableNameToCollectionMapping { get; set; }
    }
}
