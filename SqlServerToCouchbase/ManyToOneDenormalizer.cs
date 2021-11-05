using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Threading.Tasks;
using Couchbase;
using Couchbase.KeyValue;
using Dapper;
using Dynamitey;
using Humanizer;
using SqlServerToCouchbase.DatabasesFrom;

namespace SqlServerToCouchbase
{
    public class ManyToOneDenormalizer : IDenormalizer
    {
        public ManyToOneFrom From { get; set; }
        public ManyToOneTo To { get; set; }

        public string Description => $"ManyToOne Denormalize: [{From.SchemaName}.{From.TableName}] => [{To.SchemaName}.{To.TableName}]";

        public async Task DenormalizeAsync(SqlToCbConfig config, IDatabaseFrom dbFrom, IBucket bucket, SqlPipelines pipelines)
        {
            // **** get to "to" and "from" collections via scopes
            var fromCollectionName = config.GetCollectionName(From.SchemaName, From.TableName);
            var toCollectionName = config.GetCollectionName(To.SchemaName, To.TableName);

            ICouchbaseCollection fromCollection;
            ICouchbaseCollection toCollection;
            if (config.UseSchemaForScope)
            {
                var fromScopeName = config.GetScopeName(From.SchemaName);
                var fromScope = await bucket.ScopeAsync(fromScopeName);
                fromCollection = await fromScope.CollectionAsync(fromCollectionName);
                var toScopeName = config.GetScopeName(To.TableName);
                var toScope = await bucket.ScopeAsync(toScopeName);
                toCollection = await toScope.CollectionAsync(toCollectionName);
            }
            else
            {
                var defaultScope = await bucket.DefaultScopeAsync();
                fromCollection = await defaultScope.CollectionAsync(fromCollectionName);
                toCollection = await defaultScope.CollectionAsync(toCollectionName);
            }

            // **** start with each row of data from the table in SQL Server
            // lookup and use custom pipeline defined for this table
            SqlPipelineBase pipeline = new SqlPipelineDefault(From.SchemaName, From.TableName);
            var customPipeline = pipelines?.Get(From.SchemaName, From.TableName);
            if (customPipeline != null)
                pipeline = customPipeline;

            // buffered false because this could be a very large amount of data
            // see: https://dapper-tutorial.net/buffered
            using (var outerConnection = new SqlConnection(config.SourceSqlConnectionString))
            {
                var rows = outerConnection.Query(pipeline.Query, buffered: false);

                foreach (var row in rows)
                {
                    if (!pipeline.IsIncluded(row))
                        continue;

                    var rootKey = string.Join("::", From.ForeignKeyNames.Select(k => Dynamic.InvokeGet(row, k)));

                    // try to find that doc
                    var doesTargetDocExist = await toCollection.ExistsAsync(rootKey);

                    // if that doc doesn't exist, then
                    // the assumption is that it was filtered out
                    // and so, ignore it
                    if (!doesTargetDocExist.Exists)
                        continue;

                    // **** embed the corresponding document into the target doc
                    string keyForDocToEmbed = await GetDocumentKeyFromPrimaryKeyValuesAsync(row, From.SchemaName, From.TableName, config, dbFrom);
                    // TODO: could probably replace above pipeline.IsIncluded check and just assume that
                    // TODO: if doc doesn't exist, it was filtered out
                    var docToEmbed = await fromCollection.GetAsync(keyForDocToEmbed);

                    // TODO: issue here, what happens when program is run again?
                    // TODO: it might be okay as long as the root document is overwritten (upserted) again to its normalized state
                    // TODO: otherwise, might need a "reset" of all denormalized root tables at the very start
                    await toCollection.MutateInAsync(rootKey, spec =>
                    {
                        spec.ArrayAppend(From.TableName.Pluralize(), docToEmbed.ContentAs<dynamic>(), true);
                    });
                }
            }
        }

        private async Task<string> GetDocumentKeyFromPrimaryKeyValuesAsync(dynamic row, string tableSchema, string tableName, SqlToCbConfig config, IDatabaseFrom dbFrom)
        {
            // append key values together with :: delimeter
            // for compound keys
            var keys = await config.GetPrimaryKeyNames(tableSchema, tableName, dbFrom);
            var newKey = string.Join("::", keys.Select(k => Dynamic.InvokeGet(row, k)));

            // if there IS no key, generate one
            if (string.IsNullOrWhiteSpace(newKey))
                return Guid.NewGuid().ToString();

            return newKey;
        }
    }

    public class ManyToOneFrom
    {
        public string SchemaName { get; set; }
        public string TableName { get; set; }
        public List<string> ForeignKeyNames { get; set; }
    }

    public class ManyToOneTo
    {
        public string SchemaName { get; set; }
        public string TableName { get; set; }
    }
}