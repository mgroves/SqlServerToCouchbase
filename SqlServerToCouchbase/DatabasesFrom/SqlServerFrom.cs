using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Reflection;
using System.Runtime.Loader;
using System.Threading.Tasks;
using Couchbase.Management.Users;
using Dapper;
using Microsoft.Extensions.Logging;
using Microsoft.SqlServer.Types;

namespace SqlServerToCouchbase.DatabasesFrom
{
    public class SqlServerFrom : IDatabaseFrom
    {
        private readonly SqlToCbConfig _config;
        private readonly ILogger<SqlToCb> _logger;
        private SqlConnection _sqlConnection;

        public SqlServerFrom(ILoggerFactory loggerFactory, SqlToCbConfig config)
        {
            _config = config;
            _logger = loggerFactory.CreateLogger<SqlToCb>();
        }
        
        public void Initialize()
        {
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

        public void Connect()
        {
            _logger.LogInformation("Connecting to SQL Server...");
            _sqlConnection = new SqlConnection(_config.SourceSqlConnectionString);
            _logger.LogInformation("Done");
        }

        public void Dispose()
        {
            _logger.LogInformation("Disconnecting from SQL Server...");
            _sqlConnection.Close();
            _sqlConnection.Dispose();
            _logger.LogInformation("Done");
        }

        public async Task<List<dynamic>> GetListOfTables()
        {
            var tables = (await _sqlConnection.QueryAsync(@"
                select TABLE_SCHEMA, TABLE_NAME
                from INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'")).ToList();
            return tables;
        }

        public async Task<List<string>> GetKeyNames(string schemaName, string tableName)
        {
            var keyNames = (await _sqlConnection.QueryAsync<string>(@"SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                    WHERE TABLE_NAME = @tableName
                    AND TABLE_SCHEMA = @tableSchema
                    AND CONSTRAINT_NAME LIKE 'PK_%'", new { tableName, tableSchema = schemaName })).ToList();
            return keyNames;
        }

        public async Task<List<dynamic>> GetUserNames()
        {
            var users = (await _sqlConnection.QueryAsync(@"
                SELECT u.name
                FROM sys.sysusers u
                WHERE u.issqlrole = 0
                AND u.hasdbaccess = 1")).ToList();
            return users;
        }

        public async Task<List<dynamic>> GetPermissions(string userName)
        {
            _logger.LogInformation($"Processing SQL user `{userName}`...");
            var permissions = (await _sqlConnection.QueryAsync(@"
SELECT p.permission_name, SCHEMA_NAME(t.schema_id) AS SchemaName, OBJECT_NAME(p.major_id) AS TableName
  FROM sys.database_permissions p
  INNER JOIN sys.tables t ON p.major_id = t.object_id
  INNER JOIN sys.sysusers u ON USER_ID(u.name) = p.grantee_principal_id
  WHERE p.grantee_principal_id = USER_ID(@userName)
  AND  p.class_desc = 'OBJECT_OR_COLUMN'
  AND p.permission_name IN ('INSERT', 'SELECT', 'UPDATE', 'DELETE')", new { userName })).ToList();
            return permissions;
        }

        public List<Role> GetRoles(string permissionName, string scopeName, string collectionName)
        {
            var roles = new List<Role>();
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

            return roles;
        }

        public async Task<List<dynamic>> GetTableNames()
        {
            var tables = (await _sqlConnection.QueryAsync(@"
                select TABLE_SCHEMA, TABLE_NAME
                from INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'")).ToList();
            return tables;
            
            // var tables = (await _sqlConnection.QueryAsync(@"
            //     select SCHEMA_NAME(t.schema_id) AS TABLE_SCHEMA, t.name AS TABLE_NAME
            //     from sys.tables t
            //     where t.temporal_type_desc IN ('SYSTEM_VERSIONED_TEMPORAL_TABLE', 'NON_TEMPORAL_TABLE')
            // ")).ToList();
        }

        public async Task<List<dynamic>> GetIndexes()
        {
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
            return indexes;
        }

        public IDbConnection GetNewConnection(string connectionString)
        {
            return new SqlConnection(_config.SourceSqlConnectionString);
        }

        public IEnumerable<dynamic> QueryBulk(IDbConnection conn, SqlPipelineBase pipeline)
        {
            return conn.Query(pipeline.Query, buffered: false);
        }
    }
}