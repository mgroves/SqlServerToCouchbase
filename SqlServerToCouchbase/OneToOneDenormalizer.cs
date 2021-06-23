using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Couchbase;

namespace SqlServerToCouchbase
{
    public class OneToOneDenormalizer : IDenormalizer
    {
        public OneToOneFrom From { get; set; }
        public OneToOneTo To { get; set; }
        public Task DenormalizeAsync(SqlToCbConfig config, SqlConnection sqlConnection, IBucket bucket, SqlPipelines pipelines)
        {
            throw new System.NotImplementedException();
        }
    }

    public class OneToOneFrom
    {
        public string SchemaName { get; set; }
        public string TableName { get; set; }
        public List<string> ForeignKeyNames { get; set; }
    }

    public class OneToOneTo
    {
        public string SchemaName { get; set; }
        public string TableName { get; set; }
        public bool RemoveForeignKey { get; set; }
        public bool Unnest { get; set; }
        public string UnnestSeparator { get; set; } = "_";
    }
}