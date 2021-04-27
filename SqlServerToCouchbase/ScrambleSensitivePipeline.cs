using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Dynamitey;

namespace SqlServerToCouchbase
{
    /// <summary>
    /// Use this pipeline to scramble values for the given field names.
    /// Useful when working with a database with sensitive data/PII
    /// </summary>
    public class ScrambleSensitivePipeline : SqlPipelineBase
    {
        private readonly string _schemaName;
        private readonly string _tableName;
        private readonly string[] _fieldNames;
        private readonly Random _rand;

        /// <summary>
        /// Use this pipeline to scramble values for the given field names.
        /// Useful when working with a database with sensitive data/PII
        /// </summary>
        /// <param name="schemaName">Schema name</param>
        /// <param name="tableName">Table name</param>
        /// <param name="fieldNames">The name of the fields whose values you want scrambled when they reach Couchbase</param>
        public ScrambleSensitivePipeline(string schemaName, string tableName, params string[] fieldNames) : base(schemaName, tableName)
        {
            _schemaName = schemaName;
            _tableName = tableName;
            _fieldNames = fieldNames;
            _rand = new Random();
        }

        public override dynamic Transform(dynamic row)
        {
            IEnumerable<string> memberNames = Dynamic.GetMemberNames(row);
            var targetFieldNames = memberNames.Where(m => _fieldNames.Contains(m));

            foreach (var fieldName in targetFieldNames)
            {
                var val = Dynamitey.Dynamic.InvokeGet(row, fieldName); // memberNames.Keys.SingleOrDefault(k => k.Equals(fieldName, StringComparison.InvariantCultureIgnoreCase));
                switch (val)
                {
                    case int i:
                        Dynamic.InvokeSet(row, fieldName, _rand.Next(0, int.MaxValue));
                        break;
                    case long l:
                        Dynamic.InvokeSet(row, fieldName, _rand.Next(0, int.MaxValue));
                        break;
                    case short sh:
                        Dynamic.InvokeSet(row, fieldName, _rand.Next(0, short.MaxValue));
                        break;
                    case decimal d:
                        Dynamic.InvokeSet(row, fieldName, _rand.NextDecimal());
                        break;
                    case double d:
                        Dynamic.InvokeSet(row, fieldName, _rand.Next(0, val * _rand.NextDouble()));
                        break;
                    case string s:
                        Dynamic.InvokeSet(row, fieldName, RandomStringOfLength(val.ToString().Length));
                        break;
                    case DateTime dt:
                        Dynamic.InvokeSet(row, fieldName, 
                            (new DateTime(1980,1,1)
                                .AddSeconds(_rand.Next(0,60))
                                .AddMinutes(_rand.Next(1,52560000))));
                        break;
                    default:
                        throw new ArgumentException($"For table [{_schemaName}.{_tableName}], column {fieldName}, I don't know how to scramble a type of {val.GetType()} yet. Open a GitHub issue!");
                }
            }

            return row;
        }

        private string RandomStringOfLength(int length)
        {
            StringBuilder sb = new StringBuilder();
            int numGuidsToConcat = (((length - 1) / 32) + 1);
            for (int i = 1; i <= numGuidsToConcat; i++)
            {
                sb.Append(Guid.NewGuid().ToString("N"));
            }

            return sb.ToString(0, length);
        }
    }
}