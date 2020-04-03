using Newtonsoft.Json;
using Npgsql;
using NpgsqlTypes;
using System;
using System.Collections.Generic;
using System.Data;
using System.Text;

namespace PostGresSQLCopyHelper
{
    class PostgresDatabaseClient
    {
        NpgsqlConnection _connection;

        NpgsqlTransaction _transaction;
        public void BulkUpsert<T>(string index, List<T> items) where T : class, IRepositoryItem
        {
            try
            {
                this.OpenConnection();
                this.TransactionBegin();
                var TableName = typeof(T).Name;
                TableName = TableName.ToLower();
                var TempTableName = TableName + "temp";
                this.CreateTempTable(TempTableName);
                var query = PostGresQueryBuilder.BulkInsert(TempTableName);
                this.WriteDataToStream(index, items, query);
                this.InsertDataFromTempTable(index, TableName, TempTableName);
                this._transaction.Commit();
            }
            catch(NpgsqlException ex)
            {
                _transaction.Rollback();
            }
            finally
            {
                this._transaction.Dispose();
                this._transaction = null;
                this.CloseConnection();
            }
        }

        

        private void WriteDataToStream<T>(string index, List<T> Items, PostGresQueryBuilder query) where T : class, IRepositoryItem
        {
            this.OpenConnection();
            using(var writer = _connection.BeginBinaryImport(query.GetQuery()))
            {
                foreach(var Item in Items)
                {
                    writer.StartRow();
                    writer.Write(Item.GetId(), NpgsqlDbType.Varchar);
                    writer.Write(index, NpgsqlDbType.Varchar);
                    writer.Write(Item.CompanyId, NpgsqlDbType.Varchar);
                    writer.Write(Item.ProjectId, NpgsqlDbType.Varchar);
                    writer.Write(JsonConvert.SerializeObject(Item), NpgsqlDbType.Jsonb);
                }
                writer.Complete();
            }
        }

        private void CreateTempTable(string tempTableName)
        {
            var queryBuilder = PostGresQueryBuilder.CreateTemporaryTable(tempTableName);
            this.ExecuteNonQuery(queryBuilder);
        }

        private void InsertDataFromTempTable(string index, string tableName, string tempTableName)
        {
            var query = PostGresQueryBuilder.InsertDataFromTable(tempTableName, tableName, index);
            this.ExecuteNonQuery(query);
        }

        private int ExecuteNonQuery(PostGresQueryBuilder query)
        {
            int result;
            OpenConnection();
           using (var command = new NpgsqlCommand(query.GetQuery(), this._connection,this._transaction))
           {
                result = command.ExecuteNonQuery();
           }
            
            return result;
        }

        public void TransactionBegin()
        {
            if (this._transaction == null)
            {
                this._transaction = this._connection.BeginTransaction(IsolationLevel.Serializable);
            }
        }

        public NpgsqlConnection GetConnection()
        {
            return new NpgsqlConnection("Server=127.0.0.1;User Id=postgres; " + "Password=01853697360;Database=postgres;");
        }

        public void OpenConnection()
        {
            if (this._connection == null)
            {
                this._connection = GetConnection();
            }
            if (this._connection.State == ConnectionState.Closed)
            {
                this._connection.Open();
            }
        }
        public void CloseConnection()
        {
            if (this._connection != null)
            {
                if (this._connection.State == ConnectionState.Open)
                {
                    this._connection.Close();
                }
            }
        }
    }
}
