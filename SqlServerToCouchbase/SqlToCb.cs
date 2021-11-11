using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Couchbase;
using Couchbase.Core.Exceptions;
using Couchbase.Core.IO.Serializers;
using Couchbase.Diagnostics;
using Couchbase.KeyValue;
using Couchbase.Management.Buckets;
using Couchbase.Management.Collections;
using Couchbase.Management.Users;
using Dynamitey;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using SqlServerToCouchbase.DatabasesFrom;

namespace SqlServerToCouchbase
{
    public class SqlToCb : IDisposable
    {
        private readonly SqlToCbConfig _config;
        private readonly IDatabaseFrom _dbFrom;
        private readonly ILogger _logger;
        private ICluster _cluster;
        private IBucket _bucket;
        private bool _isValid;
        private ICouchbaseCollectionManager _collManager;

        public SqlToCb(SqlToCbConfig config, ILoggerFactory loggerFactory, IDatabaseFrom dbFrom)
        {
            _config = config;
            _dbFrom = dbFrom;
            _logger = loggerFactory.CreateLogger<SqlToCb>();
            _isValid = false;

            _dbFrom.Initialize();
        }

        public async Task ConnectAsync()
        {
            _dbFrom.Connect();

            _logger.LogInformation("Connecting to Couchbase...");
            var serializerSettings = new JsonSerializerSettings();
            serializerSettings.AddSqlConverters();

            var options = new ClusterOptions
            {
                UserName = _config.TargetUsername,
                Password = _config.TargetPassword,
                Serializer = new DefaultSerializer(serializerSettings, serializerSettings),
                KvIgnoreRemoteCertificateNameMismatch = true,
                HttpIgnoreRemoteCertificateMismatch = true
            };
            _cluster = await Cluster.ConnectAsync(_config.TargetConnectionString, options);
            _logger.LogInformation("Done");
        }

        public void Dispose()
        {
            try
            {
                _dbFrom.Dispose();
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

        private async Task ValidateNamesAsync()
        {
            _logger.LogInformation("Now validating names...");
            var tables = await _dbFrom.GetListOfTables();
            foreach (var table in tables)
            {
                string collectionName = _config.GetCollectionName(table.TABLE_SCHEMA, table.TABLE_NAME);
                if (collectionName.Length <= 30)
                {
                    _logger.LogInformation($"Name `{collectionName}` is fine.");
                    continue;
                }
                else
                {
                    _logger.LogError($"Name `{collectionName}` is too long. Can't continue.");
                    _isValid = false;
                    return;
                }
            }
            _logger.LogInformation("Name validation complete.");
            _isValid = true;
        }

        /// <summary>
        /// Start one or more Migration processes
        /// </summary>
        /// <param name="validateNames">Check to make sure the table names are short enough or are mapped properly</param>
        /// <param name="createBucket">Create a Couchbase bucket to match the SQL catalog</param>
        /// <param name="createCollections">Create Couchbase collections to match the SQL tables</param>
        /// <param name="createIndexes">Create Couchbase indexes to match the SQL indexes</param>
        /// <param name="copyData">Create Couchbase documents to match the SQL rows</param>
        /// <param name="sampleForDemo">Only create a sample set of Couchbase documents/indexes - only suitable for demos!</param>
        /// <param name="createUsers">Create Couchbase users to match the SQL users</param>
        /// <param name="denormalize">Perform denormalization based on the DenormalizeMaps</param>
        /// <param name="pipelines">Pipelines mapped to tables to perform filters/transforms</param>
        /// <returns></returns>
        public async Task MigrateAsync(
            bool validateNames = false,
            bool createBucket = false,
            bool createCollections = false,
            bool createIndexes = false,
            bool copyData = false,
            bool sampleForDemo = false,
            bool createUsers = false,
            bool denormalize = false,
            SqlPipelines pipelines = null)
        {
            if (validateNames) await ValidateNamesAsync();

            var shouldContinue = createBucket || createCollections || createIndexes || copyData || createUsers || denormalize;

            if (!shouldContinue)
                return;

            if (!_isValid)
                throw new Exception("Validation on SQL Server names has not been performed. Cannot continue with migration.");

            if (createBucket)
                await CreateBucketAsync();
            else
                await ConnectBucketAsync();

            if (createCollections) await CreateCollectionsAsync();

            if (createIndexes) await CreateIndexesAsync(sampleForDemo);

            if (copyData) await CopyDataAsync(sampleForDemo, pipelines);

            if (createUsers) await CreateUsersAsync();

            if (denormalize) await Denormalize(pipelines);
        }

        private async Task Denormalize(SqlPipelines pipelines)
        {
            _logger.LogInformation("Starting denormalizing.");
            var maps = _config.DenormalizeMaps;
            foreach (var map in maps)
            {
                _logger.LogInformation($"{map.Description}");
                await map.DenormalizeAsync(_config, _dbFrom, _bucket, pipelines);
                _logger.LogInformation("Complete.");
            }
        }

        private async Task CreateUsersAsync()
        {
            _logger.LogInformation("Now creating users...");
            var userManager = _cluster.Users;

            Regex regMatchSpecialCharsOnly = new Regex("[^a-zA-Z0-9']");

            var users = await _dbFrom.GetUserNames();
            foreach (var user in users)
            {
                string userName = user.name.ToString();
                string couchbaseUserName = regMatchSpecialCharsOnly.Replace(userName, "-");
                var permissions = await _dbFrom.GetPermissions(userName);

                var roles = new List<Role>();
                var couchbaseUser = new User(couchbaseUserName);
                couchbaseUser.DisplayName = couchbaseUserName;
                couchbaseUser.Password = _config.DefaultPasswordForUsers;

                foreach (var permission in permissions)
                {
                    string permissionName = permission.permission_name.ToString();
                    string schemaName = permission.SchemaName.ToString();
                    string tableName = permission.TableName.ToString();

                    var collectionName = _config.GetCollectionName(schemaName, tableName);
                    var scopeName = _config.GetScopeName(schemaName);
                    roles.AddRange(_dbFrom.GetRoles(permissionName, scopeName, collectionName));
                }

                // if there are no roles, then assume this user should have access
                // to everything in the bucket
                if(!roles.Any())
                    roles.Add(new Role("bucket_admin", _config.TargetBucket));

                couchbaseUser.Roles = roles;

                _logger.LogInformation($"Creating user `{couchbaseUser.DisplayName}`");

                await userManager.UpsertUserAsync(couchbaseUser);
            }
            _logger.LogInformation("User creation complete.");
        }

        private async Task CreateBucketAsync()
        {
            _logger.LogInformation("Attempting to create bucket...");
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
                _logger.LogInformation("Bucket creation complete.");
            }
            catch (BucketExistsException)
            {
                _logger.LogInformation("already exists.");
            }

            _bucket = await _cluster.BucketAsync(_config.TargetBucket);
            var opts = new WaitUntilReadyOptions();
            opts.DesiredState(ClusterState.Online);
            opts.ServiceTypes(ServiceType.KeyValue, ServiceType.Query);
            await _bucket.WaitUntilReadyAsync(TimeSpan.FromSeconds(30), opts);

            _collManager = _bucket.Collections;
        }

        private async Task ConnectBucketAsync()
        {
            _bucket = await _cluster.BucketAsync(_config.TargetBucket);

            _collManager = _bucket.Collections;
        }

        private async Task CreateCollectionsAsync()
        {
            _logger.LogInformation("Creating collections...");
            var tables = await _dbFrom.GetTableNames();
            foreach (var table in tables)
            {
                string collectionName = _config.GetCollectionName(table.TABLE_SCHEMA, table.TABLE_NAME);
                string scopeName = _config.GetScopeName(table.TABLE_SCHEMA);

                _logger.LogInformation($"Creating collection `{collectionName}`...");

                await CreateScopeIfNecessaryAsync(scopeName);

                if (await CollectionExistsAsync(collectionName, scopeName))
                {
                    _logger.LogInformation("already exists.");
                    continue;
                }

                try
                {
                    var spec = new CollectionSpec(scopeName, collectionName);
                    await _collManager.CreateCollectionAsync(spec);
                }
                catch
                {
                    _logger.LogError($"Unable to create collection `{collectionName}` in scope `{scopeName}`");
                    throw;
                }
                _logger.LogInformation("Done");
            }
            _logger.LogInformation("Collection creation complete.");
        }

        private async Task CreateScopeIfNecessaryAsync(string scopeName)
        {
            try
            {
                var scopes = await _bucket.Collections.GetAllScopesAsync();
                if (scopes.Any(s => s.Name == scopeName))
                    return;
                await _collManager.CreateScopeAsync(scopeName);
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Problem creating or checking Scope `{scopeName}`...");
                throw;
            }
            
            _logger.LogInformation($"Done");
        }

        private async Task<bool> CollectionExistsAsync(string collectionName, string scopeName)
        {
            try
            {
                var scopes = await _bucket.Collections.GetAllScopesAsync();
                var scope = scopes.SingleOrDefault(s => s.Name == scopeName);
                if (scope == null)
                    return false;

                var collections = scope.Collections;
                return collections.Any(c => c.Name == collectionName);

                // TODO: there might be a bug here (NBCB-2767)
                // var scope = _bucket.Scope(scopeName);
                // scope.Collection(collectionName);
                // return true;
            }
            catch (Couchbase.Core.Exceptions.CollectionNotFoundException)
            {
                return false;
            }
        }

        private async Task CreateIndexesAsync(bool sampleForDemo = false)
        {
            _logger.LogInformation($"Creating{(sampleForDemo ? " some sample" : " ")} indexes...");
            var indexes = await _dbFrom.GetIndexes();

            if (sampleForDemo)
                indexes = indexes.Take(5).ToList();

            foreach (var index in indexes)
            {
                string indexName = index.index_name.ToString();

                var collectionName = _config.GetCollectionName(index.schema_name, index.table_name);
                var scopeName = _config.GetScopeName(index.schema_name);
                _logger.LogInformation($"From Index: `{indexName}`...");
                string fields = string.Join(",", ((string)index.columns)
                    .Split(',')
                    .Select(f => $"`{f}`"));

                try
                {
                    await _cluster.QueryAsync<dynamic>(
                        $"CREATE INDEX `sql_{indexName}` ON `{_config.TargetBucket}`.`{scopeName}`.`{collectionName}` ({fields})");
                }
                catch (IndexExistsException)
                {
                    _logger.LogInformation($"Index sql_{indexName} already exists.");
                }

                _logger.LogInformation("Done.");
            }

            _logger.LogInformation("Index creation complete.");
        }

        private async Task CopyDataAsync(bool sampleData = false, SqlPipelines pipelines = null)
        {
            _logger.LogInformation("Copying data started...");

            var tables = await _dbFrom.GetTableNames();
            
            // *** refresh to workaround NCBC-2784
            // TODO: can this be removed as of 3.1.2?
            Dispose();
            await ConnectAsync();
            await ConnectBucketAsync();
            // ****

            foreach (var table in tables)
                await CopyDataFromTableToCollectionAsync(table.TABLE_SCHEMA, table.TABLE_NAME, sampleData, pipelines);

            _logger.LogInformation("Copying data complete.");
        }

        private async Task CopyDataFromTableToCollectionAsync(string tableSchema, string tableName, bool sampleData = false, SqlPipelines pipelines = null)
        {
            var collectionName = _config.GetCollectionName(tableSchema,tableName);

            _logger.LogInformation($"Copying data from `{tableSchema}.{tableName}` table to {collectionName} collection...");
            var counter = 0;

            ICouchbaseCollection collection;
            if (_config.UseSchemaForScope)
            {
                var scopeName = _config.GetScopeName(tableSchema);
                var scope = await _bucket.ScopeAsync(scopeName);
                collection = await scope.CollectionAsync(collectionName);
            }
            else
            {
                var scope = await _bucket.DefaultScopeAsync();
                collection = await scope.CollectionAsync(collectionName);
            }

            // lookup and use custom pipeline defined for this table
            SqlPipelineBase pipeline = new SqlPipelineDefault(tableSchema, tableName);
            if (sampleData)
                pipeline = new SqlPipelineDefaultSample(tableSchema, tableName);
            var customPipeline = pipelines?.Get(tableSchema, tableName);
            if (customPipeline != null)
                pipeline = customPipeline;

            // buffered false because this could be a very large amount of data
            // see: https://dapper-tutorial.net/buffered
            using (var outerConnection = _dbFrom.GetNewConnection(_config.SourceSqlConnectionString))
            {
                _logger.LogInformation($"Opening non-buffered connection to `{tableSchema}.{tableName}`");
                var rows = _dbFrom.QueryBulk(outerConnection, pipeline);

                _logger.LogInformation("Writing row(s)...");
                foreach (var row in rows)
                {
                    if (!pipeline.IsIncluded(row))
                        continue;

                    var transformedRow = pipeline.Transform(row);

                    string documentKey = null;
                    try
                    {
                        documentKey = await GetDocumentKeyFromPrimaryKeyValuesAsync(transformedRow, tableSchema, tableName);
                        await collection.UpsertAsync(documentKey, transformedRow);
                    }
                    catch (Exception ex)
                    {
                        _logger.LogError($"Error writing.");
                        _logger.LogError($"Row data: {transformedRow}");
                        _logger.LogError($"Document key: {documentKey}");
                        _logger.LogError($"Exception message: {ex.Message}");
                        _logger.LogError($"Exception stack trace: {ex.StackTrace}");
                    }

                    counter++;
                    if ((counter % 1000) == 0)
                        _logger.LogInformation(counter + "...");
                }
            }
            _logger.LogInformation($"Closing non-buffered connection to `{tableSchema}.{tableName}`");
            _logger.LogInformation("Done");
        }

        private async Task<string> GetDocumentKeyFromPrimaryKeyValuesAsync(dynamic row, string tableSchema, string tableName)
        {
            // append key values together with :: delimeter
            // for compound keys
            var keys = await _config.GetPrimaryKeyNames(tableSchema, tableName, _dbFrom);
            var newKey = string.Join("::", keys.Select(k => Dynamic.InvokeGet(row, k)));
        
            // if there IS no key, generate one
            if (string.IsNullOrWhiteSpace(newKey))
                return Guid.NewGuid().ToString();
        
            return newKey;
        }
    }
}
