using System;
using System.Collections.Generic;
using System.Text;
using Npgsql;
using NpgsqlTypes;

namespace PostGresSQLCopyHelper
{
    public class PostGresQueryBuilder
    {
        private const string QCreateTempporaryTable = "CREATE TEMPORARY TABLE IF NOT EXISTS {0}(" +
                                                      "id character varying(255) NOT NULL," +
                                                      "index character varying(100)  NOT NULL," +
                                                      "companyid character varying(255)," +
                                                      "projectid character varying(255)," +
                                                      "jsonmodel jsonb NOT NULL);";

        private const string QBulkInsert = "COPY {0}(id, index, companyid, projectid, jsonmodel) FROM STDIN BINARY;";

        private const string QInsertDataFromTable = "INSERT INTO \"{1}\".{2} (id, index, companyid, projectid, jsonmodel) " +
                                                     "SELECT DISTINCT ON(id) id, index, companyid, projectid, jsonmodel FROM {0} " +
                                                     "ON CONFLICT(id) " +
                                                     "DO UPDATE SET index = excluded.index," +
                                                     "companyid = excluded.companyid," +
                                                     "projectid = excluded.projectid," +
                                                     "jsonmodel = excluded.jsonmodel; ";
        private string Query { get; set; }

        public List<NpgsqlParameter> Parameters { get; }
        public PostGresQueryBuilder(string baseQ)
        {
            this.Parameters = new List<NpgsqlParameter>();
            this.Query = baseQ;
        }
        public static PostGresQueryBuilder CreateTemporaryTable(string tempTableName)
        {
            return new PostGresQueryBuilder(string.Format(QCreateTempporaryTable, tempTableName));
        }

        public static PostGresQueryBuilder BulkInsert(string tempTableName)
        {
            return new PostGresQueryBuilder(string.Format(QBulkInsert, tempTableName));
        }

        public static PostGresQueryBuilder InsertDataFromTable(string fromTable, string toTable,string index)
        {
            return new PostGresQueryBuilder(string.Format(QInsertDataFromTable, fromTable, index,toTable));
        }

        public string GetQuery()
        {
            return this.Query;
        }
    }
}
