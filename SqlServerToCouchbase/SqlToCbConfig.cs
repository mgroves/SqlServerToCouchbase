using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading.Tasks;
using Dapper;

namespace SqlServerToCouchbase
{
    public class SqlToCbConfig
    {
        public string SourceSqlConnectionString { get; set; }
        public string TargetConnectionString { get; set; }
        public string TargetUsername { get; set; }
        public string TargetPassword { get; set; }
        public string TargetBucket { get; set; }
        public int TargetBucketRamQuotaMB { get; set; }
        public IDictionary<string,string> TableNameToCollectionMapping { get; set; }
        public bool UseSchemaForScope { get; set; }
        public bool UseDefaultScopeForDboSchema { get; set; }
        public string DefaultPasswordForUsers { get; set; }
        public List<IDenormalizer> DenormalizeMaps { get; set; }
        private Dictionary<string, List<string>> PrimaryKeyNames;

        public async Task<List<string>> GetPrimaryKeyNames(string schemaName, string tableName, IDbConnection sqlConnection)
        {
            if (!PrimaryKeyNames.ContainsKey($"{schemaName}.{tableName}"))
            {
                var keyNames = (await sqlConnection.QueryAsync<string>(@"SELECT COLUMN_NAME
                    FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
                    WHERE TABLE_NAME = @tableName
                    AND TABLE_SCHEMA = @tableSchema
                    AND CONSTRAINT_NAME LIKE 'PK_%'", new { tableName, tableSchema = schemaName })).ToList();
                PrimaryKeyNames.Add($"{schemaName}.{tableName}", keyNames.ToList());
            }
            
            return PrimaryKeyNames[$"{schemaName}.{tableName}"];
        }

        public SqlToCbConfig()
        {
            PrimaryKeyNames = new Dictionary<string, List<string>>();
        }

        public string GetCollectionName(string tableSchema, string tableName)
        {
            string rawCollectionName;
        
            // if not using schema<-> translation, then:
            //  dbo.Foo => Foo
            //  Foo.Bar => Foo_Bar
            if (!UseSchemaForScope)
                rawCollectionName = (tableSchema == "dbo" ? "" : (tableSchema + '_')) + tableName;
            
            // otherwise:
            // dbo.Foo => _default (scope) -> Foo (collection)
            // Foo.Bar => Foo (scope) -> Bar (collection)
            else
                rawCollectionName = tableName;
        
            // use the mapping if there is one, collection names are limited to 30 characters
            var collectionName = TableNameToCollectionMapping.ContainsKey(rawCollectionName)
                ? TableNameToCollectionMapping[rawCollectionName]
                : rawCollectionName;
        
            // remove spaces--allowed in table names, not allowed in collection name
            collectionName = collectionName.Replace(" ", "-");
        
            return collectionName;
        }

        public string GetScopeName(string tableSchema)
        {
            if (!UseSchemaForScope) return "_default";
            if (UseDefaultScopeForDboSchema && tableSchema == "dbo")
                return "_default";
            return tableSchema;
        }
    }
}