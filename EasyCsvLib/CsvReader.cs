using System;
using System.IO;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using System.Linq;
using c = EasyCsvLib.Common;

namespace EasyCsvLib
{
    public interface ICsvReader
    {
        long ImportCsv();
        string Error { get; }
        DataTable DataTable { get; }
        string TableName { get; set; }
        string Delimiter { get; set; }
        string ConnectionString { get; set; }
        string FilePath { get; set; }
        bool OutputTableDefinition(string outputPath);
        bool OutputToCsv(string outputPath, char delimiter = ',');
        bool OutputToCsv(string outputPath, string delimiter = ",");
        void Dispose();  
    }

    public class CsvReader : IDisposable, ICsvReader
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

        // Matches strings and strings that have quotes around them and include embedded delimiters.
        private Regex _rxCsv = null;
        protected Regex RxCsv
        {
            get
            {
                if(_rxCsv == null)
                    _rxCsv = new Regex(_delimiter + "(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))", RegexOptions.Multiline);

                return _rxCsv;
            }
        }

        private Regex _rxStripQuotes = new Regex("(^\"|\"$)");

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

        private string _delimiter = ",";
        public string Delimiter
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

        private string _tableName = null;
        public string TableName
        {
            get
            {
                return _tableName;
            }
            set
            {
                _tableName = value;
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

        /// <summary>
        /// CSV Reader
        /// Requires column headers that match the destination table column names exactly.
        /// 1. Read CSV.
        /// 2. Convert to DataTable.
        /// 3. Write to database.
        /// </summary>
        /// <param name="path"></param>
        /// <param name="tableName"></param>
        /// <param name="connectionString"></param>
        /// <param name="delimiter"></param>
        public CsvReader(string path, string tableName, string connectionString, string delimiter = ",")
        {
            if(c.IsEmpty(path))
                _error = "Parameter 'path' is required.";
            else if (!File.Exists(path))
                _error = String.Format("{0} does not exist.", path);
            else if (c.IsEmpty(tableName))
                _error = "Parameter 'tableName' is required.";
            else if (c.IsEmpty(connectionString))
                _error = "Parameter 'connectionString' is required.";
            else if (c.IsEmpty(delimiter))
                _error = "Parameter 'delimiter' is required.";

            if (_error != null)
                throw new Exception(_error);

            _delimiter = delimiter;
            _tableName = tableName;
            _connectionString = connectionString;
            _path = path;

            string[] lines = File.ReadAllLines(path);
            if (lines.Length <= 1)
            {
                _error = string.Format("File {0} is empty.", Path.GetFileName(path));
                throw new Exception(_error);
            }

            string[] colNames = GetColNames(lines);

            CreateDataTable(colNames);
            if (_error != null)
                throw new Exception("Error creating DataTable: " + _error);

            FillDataTable(colNames, lines);
            if (_error != null)
                throw new Exception("Error filling DataTable: " + _error);
        }

        public static ICsvReader Create(string path, string tableName, string connectionString, string delimiter = ",")
        {
            return new CsvReader(path, tableName, connectionString, delimiter);
        }

        public static ICsvReader Create(string path, string tableName, string connectionString, char delimiter = ',')
        {
            return new CsvReader(path, tableName, connectionString, delimiter.ToString());
        }

        /// <summary>
        /// Gets the column names from the header in the CSV file.
        /// </summary>
        /// <param name="lines"></param>
        /// <returns></returns>
        protected virtual string[] GetColNames(string[] lines)
        {
            if (lines.Length > 0)
            {
                string[] colNames = RxCsv.Split(lines[0]);
                for (int i=0; i < colNames.Length; i++)
                    colNames[i] = _rxStripQuotes.Replace(colNames[i], "").Trim();

                return colNames;
            }

            return new string[0];
        }


        /// <summary>
        /// Creates an empty DataTable object from the imported CSV.
        /// </summary>
        /// <param name="colNames"></param>
        protected virtual void CreateDataTable(string[] colNames)
        {
            SqlCommand cmd = new SqlCommand
            {
                Connection = new SqlConnection(_connectionString),
                CommandType = CommandType.Text,
                CommandText = string.Format("SELECT TOP 1 * FROM {0}", _tableName)
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
        /// Fill the DataTable object with the CSV data.
        /// </summary>
        /// <param name="colNames"></param>
        /// <param name="lines"></param>
        protected virtual void FillDataTable(string[] colNames, string[] lines)
        {
            if(colNames.Length == 0)
                throw new Exception("colNames is empty.");

            if (_dataTable.Columns.Count == 0)
                throw new Exception("DataTable columns are empty.");

            int rowCount = 0;
            try
            {
                foreach(string line in lines)
                {
                    if (c.IsEmpty(line))
                        continue;
                    
                    if (rowCount > 0)
                    {
                        DataRow row = _dataTable.NewRow();
                        string[] values = RxCsv.Split(line);

                        if (values.Length == 0)
                            continue;

                        for(int i=0; i < colNames.Length; i++)
                        {
                            string colName = colNames[i];

                            if (_dataTable.Columns.IndexOf(colName) < 0)
                                continue;

                            string val = values[i];
                            DataColumn col = _dataTable.Columns[colName];
                            if (col == null)
                                throw new Exception("Col " + colName + " not found.");

                            WriteValue(val, row, col);
                        }

                        _dataTable.Rows.Add(row);
                    }

                    rowCount++;
                }
            }
            catch (Exception ex)
            {
                _error = ex.ToString();
            }
        }

        /// <summary>
        /// Parse and write value to the DataRow column.
        /// </summary>
        /// <param name="value"></param>
        /// <param name="row"></param>
        /// <param name="col"></param>
        protected virtual void WriteValue(string value, DataRow row, DataColumn col)
        {
            string colName = col.ColumnName;
            string val = _rxStripQuotes.Replace(value, "").Trim();

            if (c.IsEmpty(val))
            {
                row[colName] = DBNull.Value;
                return;
            }

            switch (col.DataType.FullName)
            {
                case "System.Int32":
                    int? n = c.ConvertToInt(val);
                    if (n.HasValue)
                        row[colName] = n.Value;

                    break;
                case "System.Int64":
                    long? l = c.ConvertToInt64(val);
                    if (l.HasValue)
                        row[colName] = l.Value;

                    break;
                case "System.Int16":
                    short? s = c.ConvertToInt16(val);
                    if (s.HasValue)
                        row[colName] = s.Value;

                    break;
                case "System.Decimal":
                    decimal? d = c.ConvertToDecimal(val);
                    if (d.HasValue)
                        row[colName] = d.Value;

                    break;
                case "System.Boolean":
                    bool? b = c.ConvertToBoolean(val);
                    if (b.HasValue)
                        row[colName] = b.Value;

                    break;
                case "System.DateTime":
                    DateTime? date = c.ConvertToDateTime(val);
                    if (date.HasValue)
                        row[colName] = date.Value;

                    break;
                case "System.String":
                default:
                    row[colName] = val;
                    break;
            }
        }

        /// <summary>
        /// Import the CSV to the database table via SqlBulkCopy with a transaction.
        /// </summary>
        /// <returns></returns>
        public virtual long ImportCsv()
        {

            long rowCount = 0;
            if(_dataTable.Rows.Count == 0)
            {
                _error = "DataTable is empty.";
                return rowCount;
            }

            SqlConnection conn = new SqlConnection(_connectionString);

            try
            {
                conn.Open();
                var transaction = conn.BeginTransaction();

                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn, SqlBulkCopyOptions.Default, transaction))
                {
                    bulkCopy.DestinationTableName = _tableName;
                    bulkCopy.SqlRowsCopied += (sender, eventArgs) => rowCount += eventArgs.RowsCopied;
                    bulkCopy.NotifyAfter = _dataTable.Rows.Count;

                    try
                    {
                        foreach (DataColumn col in _dataTable.Columns)
                            bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);

                        bulkCopy.WriteToServer(_dataTable);
                        transaction.Commit();
                    }
                    catch (Exception ex)
                    {
                        _error = ex.ToString();
                    }
                }
                conn.Close();
            }
            catch (Exception ex)
            {
                _error = ex.ToString();
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                    conn.Close();

                conn.Dispose();
                conn = null;
            }

            return rowCount;
        }

        /// <summary>
        /// Output a text file listing the target database table's column names and data types.
        /// </summary>
        /// <param name="outputPath"></param>
        /// <returns></returns>
        public virtual bool OutputTableDefinition(string outputPath) {
            var sb = new StringBuilder();
            sb.AppendFormat("TableName: {0}{1}", _tableName, Environment.NewLine);

            foreach (DataColumn col in _dataTable.Columns)
                sb.AppendFormat("ColumnName: {0}, DataType: {1}, AllowNulls: {2}{3}"
                    , col.ColumnName, col.DataType.FullName, (col.AllowDBNull ? "true": "false"), Environment.NewLine);

            File.WriteAllText(outputPath, sb.ToString());
            return File.Exists(outputPath);
        }

        /// <summary>
        /// Output the DataTable to a CSV. 
        /// </summary>
        /// <param name="outputPath"></param>
        /// <returns></returns>
        public virtual bool OutputToCsv(string outputPath, char delimiter = ',')
        {
            return c.OutputToCsv(_dataTable, outputPath, delimiter.ToString());
        }

        /// <summary>
        /// Output the DataTable to a CSV. 
        /// </summary>
        /// <param name="outputPath"></param>
        /// <returns></returns>
        public virtual bool OutputToCsv(string outputPath, string delimiter = ",")
        {
            return c.OutputToCsv(_dataTable, outputPath, delimiter);
        }

        /// <summary>
        /// Dispose reference objects in memory.
        /// </summary>
        public void Dispose()
        {
            ((IDisposable)_dataTable).Dispose();
        }
    }
}
