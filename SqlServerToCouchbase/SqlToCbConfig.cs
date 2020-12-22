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
    }
}