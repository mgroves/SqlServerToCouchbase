using System;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Couchbase;
using Couchbase.Core.Exceptions;
using Couchbase.Core.IO.Serializers;
using Couchbase.KeyValue;
using Couchbase.Management.Buckets;
using Couchbase.Management.Collections;
using Couchbase.Management.Users;
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
        private ICouchbaseCollectionManager _collManager;

        public SqlToCb(SqlToCbConfig config, ILoggerFactory loggerFactory)
        {
            _config = config;
            _logger = loggerFactory.CreateLogger<SqlToCb>();
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
                string collectionName = GetCollectionName(table.TABLE_SCHEMA, table.TABLE_NAME);
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
        /// <returns></returns>
        public async Task Migrate(
            bool validateNames = false,
            bool createBucket = false,
            bool createCollections = false,
            bool createIndexes = false,
            bool copyData = false,
            bool sampleForDemo = false,
            bool createUsers = false)
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

            if (createIndexes) await CreateIndexes(sampleForDemo);

            if (copyData) await CopyData(sampleForDemo);

            if (createUsers) await CreateUsers();
        }

        private async Task CreateUsers()
        {
            var userManager = _cluster.Users;

            var group = new Group("MigratedUsers");

            await userManager.UpsertGroupAsync(group);

            var users = (await _sqlConnection.QueryAsync(@"
                SELECT u.name
                FROM sys.sysusers u
                WHERE u.issqlrole = 0
                AND u.hasdbaccess = 1")).ToList();
            foreach (var user in users)
            {
                string userName = user.name.ToString();
                var permissions = (await _sqlConnection.QueryAsync(@"
SELECT p.permission_name, SCHEMA_NAME(t.schema_id) AS SchemaName, OBJECT_NAME(p.major_id) AS TableName -- class_desc, major_id, permission_name, state_desc
  FROM sys.database_permissions p
  INNER JOIN sys.tables t ON p.major_id = t.object_id
  INNER JOIN sys.sysusers u ON USER_ID(u.name) = p.grantee_principal_id
  WHERE p.grantee_principal_id = USER_ID(@userName)
  AND  p.class_desc = 'OBJECT_OR_COLUMN'
  AND p.permission_name IN ('INSERT', 'SELECT', 'UPDATE', 'DELETE')", new { userName })).ToList();

                var roles = new List<Role>();
                var couchbaseUser = new User(userName);
                couchbaseUser.DisplayName = userName;
                couchbaseUser.Password = _config.DefaultPasswordForUsers;
                couchbaseUser.Groups = new List<string> { "MigratedUsers" };

                foreach (var permission in permissions)
                {
                    string permissionName = permission.permission_name.ToString();
                    string schemaName = permission.SchemaName.ToString();
                    string tableName = permission.TableName.ToString();

                    var collectionName = GetCollectionName(schemaName, tableName);
                    var scopeName = GetScopeName(schemaName);
                    switch (permissionName)
                    {
                        case "INSERT":
                            roles.Add(new Role("insert???", _config.TargetBucket, scopeName, collectionName));
                            break;
                        case "SELECT":
                            roles.Add(new Role("select???", _config.TargetBucket, scopeName, collectionName));
                            break;
                        case "UPDATE":
                            roles.Add(new Role("update???", _config.TargetBucket, scopeName, collectionName));
                            break;
                        case "DELETE":
                            roles.Add(new Role("delete???", _config.TargetBucket, scopeName, collectionName));
                            break;
                        default:
                            _logger.LogWarning($"Permission name was `{permissionName}`, which is not INSERT, SELECT, UPDATE, DELETE. It's not being copied over to Couchbase");
                            break;
                    }
                }

                couchbaseUser.Roles = roles;

                await userManager.UpsertUserAsync(user);
            }
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

            _collManager = _bucket.Collections;
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
            foreach (var table in tables)
            {
                string collectionName = GetCollectionName(table.TABLE_SCHEMA, table.TABLE_NAME);
                string scopeName = GetScopeName(table.TABLE_SCHEMA);

                _logger.LogInformation($"Creating collection `{collectionName}`...");

                await CreateScopeIfNecessary(scopeName);

                if (CollectionExists(collectionName, scopeName))
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
        }

        private async Task CreateScopeIfNecessary(string scopeName)
        {
            try
            {
                _bucket.Scope(scopeName);
            }
            catch (ScopeNotFoundException)
            {
                _logger.LogInformation($"Creating Scope `{scopeName}`...");
                var spec = new ScopeSpec(scopeName);
                await _collManager.CreateScopeAsync(spec);
            }
            
            _logger.LogInformation($"Done");
        }

        private string GetScopeName(string tableSchema)
        {
            if (!_config.UseSchemaForScope) return "_default";
            if (_config.UseDefaultScopeForDboSchema && tableSchema == "dbo")
                return "_default";
            return tableSchema;
        }

        private string GetCollectionName(string tableSchema, string tableName)
        {
            string rawCollectionName;

            // if not using schema<-> translation, then:
            //  dbo.Foo => Foo
            //  Foo.Bar => Foo_Bar
            if (!_config.UseSchemaForScope)
                rawCollectionName = (tableSchema == "dbo" ? "" : (tableSchema + '_')) + tableName;
            
            // otherwise:
            // dbo.Foo => _default (scope) -> Foo (collection)
            // Foo.Bar => Foo (scope) -> Bar (collection)
            else
                rawCollectionName = tableName;

            // use the mapping if there is one, collection names are limited to 30 characters
            var collectionName = _config.TableNameToCollectionMapping.ContainsKey(rawCollectionName)
                ? _config.TableNameToCollectionMapping[rawCollectionName]
                : rawCollectionName;

            // remove spaces--allowed in table names, not allowed in collection name
            collectionName = collectionName.Replace(" ", "-");

            return collectionName;
        }

        private bool CollectionExists(string collectionName, string scopeName)
        {
            try
            {
                // TODO: there might be a bug here (NBCB-2767)
                var scope = _bucket.Scope(scopeName);
                scope.Collection(collectionName);
                return true;
            }
            catch (Couchbase.Core.Exceptions.CollectionNotFoundException)
            {
                return false;
            }
        }

        private async Task CreateIndexes(bool sampleForDemo = false)
        {
            _logger.LogInformation("Creating indexes...");
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
                indexes = indexes.Take(25).ToList();

            foreach (var index in indexes)
            {
                string indexName = index.index_name.ToString();

                var collectionName = GetCollectionName(index.schema_name, index.table_name);
                var scopeName = GetScopeName(index.schema_name);
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
        }

        private async Task CopyData(bool sampleData = false)
        {
            _primaryKeyNames = new Dictionary<string, List<string>>();

            var tables = (await _sqlConnection.QueryAsync(@"
                select SCHEMA_NAME(t.schema_id) AS TABLE_SCHEMA, t.name AS TABLE_NAME
                from sys.tables t
                where t.temporal_type_desc IN ('SYSTEM_VERSIONED_TEMPORAL_TABLE', 'NON_TEMPORAL_TABLE')
            ")).ToList();

            foreach (var table in tables)
                await CopyDataFromTableToCollection(table.TABLE_SCHEMA, table.TABLE_NAME, sampleData);
        }

        private async Task CopyDataFromTableToCollection(string tableSchema, string tableName, bool sampleData = false)
        {
            var collectionName = GetCollectionName(tableSchema,tableName);

            _logger.LogInformation($"Copying data from `{tableSchema}.{tableName}` table to {collectionName} collection...");
            var counter = 0;

            var collection = _bucket.Collection(collectionName);
            var rows = _sqlConnection.Query($@"
                SELECT {(sampleData ? "TOP 100" : "")} *
                FROM [{tableSchema}].[{tableName}]");

            _logger.LogInformation("Writing row(s)...");
            foreach (var row in rows)
            {
                string documentKey = null;
                try
                {
                    documentKey = await GetDocumentKeyFromPrimaryKeyValues(row, tableSchema, tableName);
                    await collection.UpsertAsync(documentKey, row);
                }
                catch(Exception ex)
                {
                    _logger.LogError($"Error writing.");
                    _logger.LogError($"Row data: {row}");
                    _logger.LogError($"Document key: {documentKey}");
                    _logger.LogError($"Exception message: {ex.Message}");
                    _logger.LogError($"Exception stack trace: {ex.StackTrace}");
                }
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

            // append key values together with :: delimeter
            // for compound keys
            var keys = _primaryKeyNames[$"{tableSchema}.{tableName}"];
            var newKey = string.Join("::", keys.Select(k => Dynamic.InvokeGet(row, k)));

            // if there IS no key, generate one
            if (string.IsNullOrWhiteSpace(newKey))
                return Guid.NewGuid().ToString();

            return newKey;
        }
    }
}
