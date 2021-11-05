using System.Collections;
using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Couchbase;
using SqlServerToCouchbase.DatabasesFrom;

namespace SqlServerToCouchbase
{
    public interface IDenormalizer
    {
        string Description { get; }
        Task DenormalizeAsync(SqlToCbConfig config, IDatabaseFrom dbFrom, IBucket bucket, SqlPipelines pipelines);
    }
}