using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Data.SqlClient;
using c = EasyCsvLib.Common;

namespace EasyCsvLib
{
    public interface ICsvWriter
    {
        bool OutputToCsv(char delimiter = ',');
        string Error { get; }
        DataTable DataTable { get; }
        char Delimiter { get; set; }
        string ConnectionString { get; set; }
        string FilePath { get; set; }
        string QueryString { get; set; }
        bool IsStoredProcedure { get; set; }
        void Dispose();
    }

    public class CsvWriter : IDisposable, ICsvWriter
    {
        private string _error = null;
        public string Error
        {
            get
            {
                return _error;
            }
            private set
            {
                _error = value;
            }
        }

        private DataTable _dataTable = new DataTable();
        public DataTable DataTable
        {
            get
            {
                return _dataTable;
            }
            private set
            {
                _dataTable = value;
            }
        }

        private char _delimiter = ',';
        public char Delimiter
        {
            get
            {
                return _delimiter;
            }
            set
            {
                _delimiter = value;
            }
        }

        private string _connectionString = null;
        public string ConnectionString
        {
            get
            {
                return _connectionString;
            }
            set
            {
                _connectionString = value;
            }
        }

        private string _path = null;
        public string FilePath
        {
            get
            {
                return _path;
            }
            set
            {
                _path = value;
            }
        }

        private string _queryString = null;
        public string QueryString
        {
            get
            {
                return _queryString;
            }
            set
            {
                _queryString = value;
            }
        }

        private bool _isStoredProcedure = false;
        public bool IsStoredProcedure
        {
            get
            {
                return _isStoredProcedure;
            }
            set
            {
                _isStoredProcedure = value;
            }
        }

        /// <summary>
        /// CSV Writer
        /// Output results of an SQL query to a CSV.
        /// </summary>
        /// <param name="path"></param>
        /// <param name="connectionString"></param>
        /// <param name="delimiter"></param>
        /// <param name="queryString"></param>
        private CsvWriter(string path, string connectionString, string queryString, char delimiter = ',', bool isStoredProcedure = false)
        {
            if (c.IsEmpty(path))
                _error = "Parameter 'path' is required.";
            else if (c.IsEmpty(connectionString))
                _error = "Parameter 'connectionString' is required.";
            else if(c.IsEmpty(queryString))
                _error = "Parameter 'queryString' is required.";

            if (_error != null)
                throw new Exception(_error);

            _path = path; 
            _connectionString = connectionString;       
            _queryString = queryString;
            _delimiter = delimiter;

            GetData();

            if(_error != null)
                throw new Exception(_error);

            if (_dataTable.Rows.Count == 0)
            {
                _error = "The target table is empty.";
                throw new Exception(_error);
            }
        }

        public static ICsvWriter Create(string path, string connectionString, string queryString, char delimiter = ',', bool isStoredProcedure = false)
        {
            return new CsvWriter(path, connectionString, queryString, delimiter);
        }

        protected virtual void GetData()
        {
            SqlCommand cmd = new SqlCommand()
            {
                Connection = new SqlConnection(_connectionString),
                CommandType = _isStoredProcedure ? CommandType.StoredProcedure : CommandType.Text,
                CommandText = _queryString
            };

            SqlDataAdapter da = new SqlDataAdapter(cmd);

            try
            {
                cmd.Connection.Open();
                da.Fill(_dataTable);
                cmd.Connection.Close();
            }
            catch (Exception ex)
            {
                _error = ex.ToString();
            }
            finally
            {
                da.Dispose();
                da = null;

                if (cmd.Connection.State == ConnectionState.Open)
                    cmd.Connection.Close();

                cmd.Dispose();
                cmd = null;
            }
        }

        /// <summary>
        /// Output the DataTable to a CSV. 
        /// </summary>
        /// <returns></returns>
        public virtual bool OutputToCsv(char delimiter = ',')
        {
            return c.OutputToCsv(_dataTable, _path, delimiter);
        }

        public void Dispose()
        {
            ((IDisposable)_dataTable).Dispose();
        }
    }
}
