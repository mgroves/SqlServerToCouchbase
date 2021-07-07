using System.Collections;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Couchbase;

namespace SqlServerToCouchbase
{
    public interface IDenormalizer
    {
        string Description { get; }
        Task DenormalizeAsync(SqlToCbConfig config, SqlConnection sqlConnection, IBucket bucket, SqlPipelines pipelines);
    }
}