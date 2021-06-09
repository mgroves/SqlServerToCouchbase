using System.Collections.Generic;

namespace SqlServerToCouchbase
{
    public class DenormalizeMap
    {
        public string RootSchema { get; set; }
        public string RootTable { get; set; }
        public List<DenormalizeTable> DenormalizeTables { get; set; }
    }

    public class DenormalizeTable
    {
        public string SchemaName { get; set; }
        public string TableName { get; set; }
        public List<string> ForeignKeyNames { get; set; }
    }
}