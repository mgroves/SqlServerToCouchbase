using System;
using System.Collections.Generic;

namespace SqlServerToCouchbase
{
    public class SqlPipelines
    {
        private readonly Dictionary<string, SqlPipelineBase> _pipelines;

        public SqlPipelines()
        {
            _pipelines = new Dictionary<string, SqlPipelineBase>();
        }

        public void Add(SqlPipelineBase pipeline)
        {
            var key = $"{pipeline.SchemaName}_{pipeline.TableName}";
            if (_pipelines.ContainsKey(key))
                throw new ArgumentException($"There's already a pipeline defined for `{key}`");

            _pipelines.Add(key, pipeline);
        }

        public SqlPipelineBase Get(string schemaName, string tableName)
        {
            var key = $"{schemaName}_{tableName}";
            if (_pipelines.ContainsKey(key))
                return _pipelines[key];
            return null;
        }
    }
}