using System;
using System.Collections.Generic;

namespace SqlServerToCouchbase
{
    /// <summary>
    /// All the transform/filter pipelines you want to use
    /// on the migration
    /// </summary>
    public class SqlPipelines
    {
        private readonly Dictionary<string, SqlPipelineBase> _pipelines;

        public SqlPipelines()
        {
            _pipelines = new Dictionary<string, SqlPipelineBase>();
        }

        /// <summary>
        /// Add a pipeline. Each Schema.Table only supports one
        /// pipeline max.
        /// </summary>
        /// <param name="pipeline"></param>
        public void Add(SqlPipelineBase pipeline)
        {
            var key = $"{pipeline.SchemaName}_{pipeline.TableName}";
            if (_pipelines.ContainsKey(key))
                throw new ArgumentException($"There's already a pipeline defined for `{key}`");

            _pipelines.Add(key, pipeline);
        }

        /// <summary>
        /// Get the pipeline for the given Schema.Table, if it exists.
        /// </summary>
        /// <param name="schemaName">Schema name</param>
        /// <param name="tableName">Table name</param>
        /// <returns>Returns a pipeline or null.</returns>
        public SqlPipelineBase Get(string schemaName, string tableName)
        {
            var key = $"{schemaName}_{tableName}";
            if (_pipelines.ContainsKey(key))
                return _pipelines[key];
            return null;
        }
    }
}