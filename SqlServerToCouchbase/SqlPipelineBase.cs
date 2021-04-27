using System.Runtime.CompilerServices;

[assembly: InternalsVisibleTo("SqlServerToCouchbase.Tests")]
namespace SqlServerToCouchbase
{
    public abstract class SqlPipelineBase
    {
        public string SchemaName { get; private set; }
        public string TableName { get; private set; }

        protected SqlPipelineBase(string schemaName, string tableName)
        {
            // select every column and every row
            SchemaName = schemaName;
            TableName = tableName;
        }

        public virtual string Query => $@"SELECT * FROM [{SchemaName}].[{TableName}]";

        public virtual bool IsIncluded(dynamic row)
        {
            return true;
        }

        public virtual dynamic Transform(dynamic row)
        {
            return row;
        }
    }

    /// <summary>
    /// Default pipeline which selects all rows, all fields, and doesn't change them
    /// </summary>
    internal class SqlPipelineDefault : SqlPipelineBase
    {
        public SqlPipelineDefault(string schemaName, string tableName) : base(schemaName, tableName)
        {
        }
    }

    /// <summary>
    /// Pipeline uses to select just a sampling of data (for demo purposes)
    /// </summary>
    internal class SqlPipelineDefaultSample : SqlPipelineBase
    {
        public SqlPipelineDefaultSample(string schemaName, string tableName) : base(schemaName, tableName)
        {
        }

        public override string Query => $@"SELECT TOP 100 * FROM [{SchemaName}].[{TableName}]";
    }
}