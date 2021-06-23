using System.Collections.Generic;

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
        public Dictionary<string, List<string>> PrimaryKeyNames;

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