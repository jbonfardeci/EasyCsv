using System;
using System.Data;
using System.Data.SqlClient;
using System.IO;

namespace EasyCsvLib
{
    public class Executioner
    {
        public int ExecuteQuery(string connectionString, string filePath)
        {
            if (!File.Exists(filePath))
                throw new Exception(string.Format("File at {0} does not exist.", filePath));

            string sql = File.ReadAllText(filePath);
            return ExecuteQuery(connectionString: connectionString, sql: sql, isStoredProcedure: false);
        }

        public int ExecuteQuery(string connectionString, string sql, bool isStoredProcedure = false)
        {
            int results = 0;

            if (string.IsNullOrEmpty(connectionString))
                throw new Exception("'connectionString' is a required paraameter.");
            else if(string.IsNullOrEmpty(sql))
                throw new Exception("'sql' is a required paraameter.");

            var cmd = new SqlCommand()
            {
                CommandType = isStoredProcedure ? CommandType.StoredProcedure : CommandType.Text,
                Connection = new SqlConnection(connectionString),
                CommandText = sql
            };

            try
            {
                cmd.Connection.Open();
                results = cmd.ExecuteNonQuery();
                cmd.Connection.Close();
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                if (cmd.Connection.State == ConnectionState.Open)
                    cmd.Connection.Close();

                cmd.Dispose();
                cmd = null;
            }

            return results;
        }
    }
}
