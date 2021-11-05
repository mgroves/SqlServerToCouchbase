using System.Collections.Generic;
using System.Data;
using System.Threading.Tasks;
using Couchbase.Management.Users;

namespace SqlServerToCouchbase.DatabasesFrom
{
    public interface IDatabaseFrom
    {
        void Initialize();
        void Connect();
        void Dispose();
        Task<List<dynamic>> GetListOfTables();
        Task<List<string>> GetKeyNames(string schemaName, string tableName);
        Task<List<dynamic>> GetUserNames();
        Task<List<dynamic>> GetPermissions(string userName);
        List<Role> GetRoles(string permissionName, string scopeName, string collectionName);
        Task<List<dynamic>> GetTableNames();
        Task<List<dynamic>> GetIndexes();
        IDbConnection GetNewConnection(string connectionString);
        IEnumerable<dynamic> QueryBulk(IDbConnection conn, SqlPipelineBase pipelineQuery);
    }
}