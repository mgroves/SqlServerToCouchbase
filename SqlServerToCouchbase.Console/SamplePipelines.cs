using System;

namespace SqlServerToCouchbase.Console
{
    /// <summary>
    /// SAMPLE
    /// This pipeline will include only rows newer than a given date
    /// It uses C# to do the filtering
    /// Which means all data will still be queried from the table anyway
    /// </summary>
    public class MyCustomCsharpFilter : SqlPipelineBase
    {
        public MyCustomCsharpFilter(string schemaName, string tableName) : base(schemaName, tableName)
        {
        }

        public override bool IsIncluded(dynamic row)
        {
            DateTime modifiedDate = row.ModifiedDate;
            return modifiedDate >= new DateTime(2014, 05, 26);
        }
    }

    /// <summary>
    /// SAMPLE
    /// This pipeline overrides the default query
    /// to filter only rows that that are newer than a given date
    /// It uses SQL to do the filtering, which means
    /// the work will be pushed to SQL Server.
    /// A transform can also be applied this way instead of using "SELECT *"
    /// if you wish to transform within the SQL query
    /// </summary>
    public class ModifiedDateSqlFilter : SqlPipelineBase
    {
        private readonly DateTime _modifiedDate;
        private readonly string _schemaName;
        private readonly string _tableName;

        /// <summary>
        /// This pipeline will filter out (not copy over) any data
        /// in the specified Schema.Table that has a ModifiedDate
        /// >= the specified modifiedDate
        /// </summary>
        /// <param name="modifiedDate">Only include data >= this date</param>
        /// <param name="schemaName">Schema name</param>
        /// <param name="tableName">Table name</param>
        public ModifiedDateSqlFilter(DateTime modifiedDate, string schemaName, string tableName) : base(schemaName, tableName)
        {
            _modifiedDate = modifiedDate;
            _schemaName = schemaName;
            _tableName = tableName;
        }

        public override string Query => $@"
            SELECT *
            FROM [{_schemaName}].[{_tableName}]
            WHERE ModifiedDate >= '{_modifiedDate:yyyy-MM-dd}'";
    }


    /// <summary>
    /// SAMPLE
    /// This pipeline overrides the default transform
    /// to replace "Blvd." with "Boulevard" in all AddressLine1 fields
    /// It will do this transformation in C#
    /// </summary>
    public class MyCustomCsharpTransform : SqlPipelineBase
    {
        public MyCustomCsharpTransform(string schemaName, string tableName) : base(schemaName, tableName)
        {
        }

        public override dynamic Transform(dynamic row)
        {
            string addressLine1 = row.AddressLine1;
            row.AddressLine1 = addressLine1.Replace("Blvd.", "Boulevard");
            return row;
        }
    }
}