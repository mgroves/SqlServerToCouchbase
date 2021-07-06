using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Runtime.Loader;
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
using Dapper;
using Dynamitey;
using Microsoft.Extensions.Logging;
using Microsoft.SqlServer.Types;
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
        private bool _isValid;
        private ICouchbaseCollectionManager _collManager;

        public SqlToCb(SqlToCbConfig config, ILoggerFactory loggerFactory)
        {
            _config = config;
            _logger = loggerFactory.CreateLogger<SqlToCb>();
            _isValid = false;

            #region for use with dotMorten.Microsoft.SqlServer.Types
            // SEE: https://stackoverflow.com/questions/57012534/cant-cast-sqlgeography-when-withdrawing-data-from-db/57373622#57373622
            // AND: https://github.com/dotMorten/Microsoft.SqlServer.Types/issues/63
            AssemblyLoadContext.Default.Resolving += OnAssemblyResolve;

            Assembly OnAssemblyResolve(AssemblyLoadContext assemblyLoadContext, AssemblyName assemblyName)
            {
                try
                {
                    AssemblyLoadContext.Default.Resolving -= OnAssemblyResolve;
                    return assemblyLoadContext.LoadFromAssemblyName(assemblyName);
                }
                catch
                {
                    if (assemblyName.Name == "Microsoft.SqlServer.Types")
                        return typeof(SqlGeography).Assembly;
                    throw;
                }
                finally
                {
                    AssemblyLoadContext.Default.Resolving += OnAssemblyResolve;
                }
            }
            #endregion
        }

        public async Task ConnectAsync()
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

        private async Task ValidateNamesAsync()
        {
            _logger.LogInformation("Now validating names...");
            var tables = (await _sqlConnection.QueryAsync(@"
                select TABLE_SCHEMA, TABLE_NAME
                from INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'")).ToList();
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
            var maps = _config.DenormalizeMaps;
            foreach (var map in maps)
            {
                await map.DenormalizeAsync(_config, _sqlConnection, _bucket, pipelines);
            }
        }

        private async Task CreateUsersAsync()
        {
            _logger.LogInformation("Now creating users...");
            var userManager = _cluster.Users;

            Regex regMatchSpecialCharsOnly = new Regex("[^a-zA-Z0-9']");

            var users = (await _sqlConnection.QueryAsync(@"
                SELECT u.name
                FROM sys.sysusers u
                WHERE u.issqlrole = 0
                AND u.hasdbaccess = 1")).ToList();
            foreach (var user in users)
            {
                _logger.LogInformation($"Processing SQL user `{user.name}`...");
                string userName = user.name.ToString();
                string couchbaseUserName = regMatchSpecialCharsOnly.Replace(userName, "-");
                var permissions = (await _sqlConnection.QueryAsync(@"
SELECT p.permission_name, SCHEMA_NAME(t.schema_id) AS SchemaName, OBJECT_NAME(p.major_id) AS TableName
  FROM sys.database_permissions p
  INNER JOIN sys.tables t ON p.major_id = t.object_id
  INNER JOIN sys.sysusers u ON USER_ID(u.name) = p.grantee_principal_id
  WHERE p.grantee_principal_id = USER_ID(@userName)
  AND  p.class_desc = 'OBJECT_OR_COLUMN'
  AND p.permission_name IN ('INSERT', 'SELECT', 'UPDATE', 'DELETE')", new { userName })).ToList();

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
                    switch (permissionName)
                    {
                        case "INSERT":
                            roles.Add(new Role("query_insert", _config.TargetBucket, scopeName, collectionName));
                            roles.Add(new Role("data_writer", _config.TargetBucket, scopeName, collectionName));
                            break;
                        case "SELECT":
                            roles.Add(new Role("query_select", _config.TargetBucket, scopeName, collectionName));
                            roles.Add(new Role("data_reader", _config.TargetBucket, scopeName, collectionName));
                            break;
                        case "UPDATE":
                            roles.Add(new Role("query_update", _config.TargetBucket, scopeName, collectionName));
                            roles.Add(new Role("data_writer", _config.TargetBucket, scopeName, collectionName));
                            break;
                        case "DELETE":
                            roles.Add(new Role("query_delete", _config.TargetBucket, scopeName, collectionName));
                            roles.Add(new Role("data_writer", _config.TargetBucket, scopeName, collectionName));
                            break;
                        default:
                            _logger.LogWarning($"Permission name was `{permissionName}`, which is not INSERT, SELECT, UPDATE, DELETE. It's not being copied over to Couchbase");
                            break;
                    }
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
        }

        private async Task CreateCollectionsAsync()
        {
            _logger.LogInformation("Creating collections...");
            var tables = (await _sqlConnection.QueryAsync(@"
                select TABLE_SCHEMA, TABLE_NAME
                from INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'")).ToList();
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
            var indexes = (await _sqlConnection.QueryAsync(@"
                select i.[name] as index_name,
                    substring(column_names, 1, len(column_names)-1) as [columns],
                    schema_name(t.schema_id) AS schema_name, 
					t.[name] as table_name
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
                ")).ToList();

            if (sampleForDemo)
                indexes = indexes.Take(5).ToList();

            foreach (var index in indexes)
            {
                string indexName = index.index_name.ToString();

                var collectionName = _config.GetCollectionName(index.schema_name, index.table_name);
                var scopeName = _config.GetScopeName(index.schema_name);
                _logger.LogInformation($"SQL Server Index: `{indexName}`...");
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
            

            var tables = (await _sqlConnection.QueryAsync(@"
                select SCHEMA_NAME(t.schema_id) AS TABLE_SCHEMA, t.name AS TABLE_NAME
                from sys.tables t
                where t.temporal_type_desc IN ('SYSTEM_VERSIONED_TEMPORAL_TABLE', 'NON_TEMPORAL_TABLE')
            ")).ToList();

            // *** refresh to workaround NCBC-2784
            // TODO: can this be removed as of 3.1.2?
            Dispose();
            await ConnectAsync();
            await CreateBucketAsync();
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
            using (var outerConnection = new SqlConnection(_config.SourceSqlConnectionString))
            {
                _logger.LogInformation($"Opening non-buffered connection to `{tableSchema}.{tableName}`");
                var rows = outerConnection.Query(pipeline.Query, buffered: false);

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
            var keys = await _config.GetPrimaryKeyNames(tableSchema, tableName, _sqlConnection);
            var newKey = string.Join("::", keys.Select(k => Dynamic.InvokeGet(row, k)));
        
            // if there IS no key, generate one
            if (string.IsNullOrWhiteSpace(newKey))
                return Guid.NewGuid().ToString();
        
            return newKey;
        }
    }
}
