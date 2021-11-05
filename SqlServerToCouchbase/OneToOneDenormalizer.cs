using System.Collections.Generic;
using System.Data.SqlClient;
using System.Threading.Tasks;
using Couchbase;
using System;
using System.Data;
using System.Linq;
using Couchbase.KeyValue;
using Dapper;
using Dynamitey;
using Newtonsoft.Json.Linq;
using Microsoft.Extensions.Logging;
using SqlServerToCouchbase.DatabasesFrom;

namespace SqlServerToCouchbase
{
    public class OneToOneDenormalizer : IDenormalizer
    {
        private const string DEFAULT_UNNEST_SEPARATOR = "_";
        public OneToOneFrom From { get; set; }
        public OneToOneTo To { get; set; }

        public string Description =>
            $"OneToOne Denormalize: [{From.SchemaName}.{From.TableName}] => [{To.SchemaName}.{To.TableName}] (Unnest: {To.Unnest}, Remove Foreign Key: {To.RemoveForeignKey})";

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
                var toScopeName = config.GetScopeName(To.SchemaName);
                var toScope = await bucket.ScopeAsync(toScopeName);
                toCollection = await toScope.CollectionAsync(toCollectionName);
            }
            else
            {
                var defaultScope = await bucket.DefaultScopeAsync();
                fromCollection = await defaultScope.CollectionAsync(fromCollectionName);
                toCollection = await defaultScope.CollectionAsync(toCollectionName);
            }
            
            // go through each record in "To"
            // **** start with each row of data from the table in SQL Server
            // lookup and use custom pipeline defined for this table
            SqlPipelineBase pipeline = new SqlPipelineDefault(To.SchemaName, To.TableName);
            var customPipeline = pipelines?.Get(To.SchemaName, To.TableName);
            if (customPipeline != null)
                pipeline = customPipeline;            

            // buffered false because this could be a very large amount of data
            // see: https://dapper-tutorial.net/buffered
            using (var outerConnection = new SqlConnection(config.SourceSqlConnectionString))
            {
                // TODO: similar issue to ManyToOneDenormalizer--
                // TODO: what if this is run again, after denormalization. Will it fail, or cause dupe data, or what?
                var rows = outerConnection.Query(pipeline.Query, buffered: false);

                foreach (var row in rows)
                {
                    if (!pipeline.IsIncluded(row))
                        continue;
                    
                    // get the "To" document via ForeignKey
                    string toKey = await GetDocumentKeyFromPrimaryKeyValuesAsync(row, To.SchemaName, To.TableName, config, dbFrom);
                    var toDoc = await toCollection.GetAsync(toKey);


                    // get the "From" document
                    var fromKeys = To.ForeignKeyNames;
                    var fromCombinedKey = string.Join("::", fromKeys.Select(k => Dynamic.InvokeGet(row, k)));
                    var fromDoc = await fromCollection.GetAsync(fromCombinedKey);
                    
                    // embed that "From" document into the "To" document (unnest if configured)
                    if (To.Unnest)
                    {
                        var unnestSeparator = To.UnnestSeparator ?? DEFAULT_UNNEST_SEPARATOR;
                        var prepend = From.TableName + unnestSeparator;
                        // put From document properties at root level with from prepend + field name
                        var obj = fromDoc.ContentAs<JObject>();
                        await toCollection.MutateInAsync(toKey, specs =>
                        {
                            foreach (var kvp in obj)
                            {
                                specs.Insert(prepend + kvp.Key, kvp.Value);
                            }
                        });
                    }
                    else
                    {
                        // put From document in a nested object
                        var nestedObjectName = From.TableName;
                        var obj = fromDoc.ContentAs<dynamic>();
                        await toCollection.MutateInAsync(toKey, specs =>
                        {
                            specs.Insert(nestedObjectName, obj);
                        });
                    }

                    // remove "foreign key" from "To" document (if configured)
                    if (To.RemoveForeignKey)
                    {
                        await toCollection.MutateInAsync(toKey, specs =>
                        {
                            foreach (var fk in To.ForeignKeyNames)
                            {
                                specs.Remove(fk);
                            }
                        });
                    }
                }
            }

            
        }
        
        private async Task<string> GetDocumentKeyFromPrimaryKeyValuesAsync(dynamic row, string tableSchema, string tableName, SqlToCbConfig config, IDatabaseFrom dbFrom)
        {
            // append key values together with :: delimeter
            // for compound keys
            var keys = await config.GetPrimaryKeyNames(tableSchema, tableName, dbFrom); // .PrimaryKeyNames[$"{tableSchema}.{tableName}"];
            var newKey = string.Join("::", keys.Select(k => Dynamic.InvokeGet(row, k)));

            // if there IS no key, generate one
            if (string.IsNullOrWhiteSpace(newKey))
                return Guid.NewGuid().ToString();

            return newKey;
        }
    }

    public class OneToOneFrom
    {
        public string SchemaName { get; set; }
        public string TableName { get; set; }
    }

    public class OneToOneTo
    {
        public string SchemaName { get; set; }
        public string TableName { get; set; }
        public bool RemoveForeignKey { get; set; }
        public bool Unnest { get; set; }
        public string UnnestSeparator { get; set; } = "_";
        public List<string> ForeignKeyNames { get; set; }
    }
}