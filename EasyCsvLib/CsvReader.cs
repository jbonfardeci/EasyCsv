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

        #region class vars

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

        private int _headerRowCount = 0;
        public int HeaderRowCount
        {
            get { return _headerRowCount; }
            set { _headerRowCount = value; }
        }

        private string[] _colNames = null;
        public string[] ColNames
        {
            get { return this._colNames; }
            set { this._colNames = value; }
        }

        private int _batchSize = 1000;
        public int BatchSize {
            get { return _batchSize; }
            set { _batchSize = value; }
        }

        private int _timeOut = 300;
        public int TimeOut
        {
            get { return _timeOut; }
            set { _timeOut = value; }
        }

        public long TotalDataRows { get; set; }
        public long RowsWritten { get; set; }

        public int BatchCount { get; set; }

        public bool _verbose = false;
        public bool Verbose
        {
            get { return _verbose;  }
            set { _verbose = value; }
        }

        #endregion

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
        public CsvReader(string path, string tableName, string connectionString, string delimiter = ",", int headerRowCount = 1, string colNames = null, int batchSize = 1000, int timeOut = 300)
        {
            if(c.IsEmpty(path))
                _error = "Parameter 'path' is required.";
            else if (!File.Exists(path))
                _error = String.Format("{0} does not exist.", path);
            else if (c.IsEmpty(tableName))
                _error = "Parameter 'tableName' is required.";
            else if (c.IsEmpty(connectionString))
                _error = "Parameter 'connectionString' is required.";

            if (_error != null)
                throw new Exception(_error);

            _path = path;
            _tableName = tableName;
            _connectionString = connectionString;
            _delimiter = delimiter;
            _colNames = !c.IsEmpty(colNames) ? c.SplitColNames(colNames, _delimiter) : c.GetColNamesFromCsv(_path, _delimiter);
            _batchSize = batchSize;
            _timeOut = timeOut;
            _headerRowCount = headerRowCount;
        }

        public static ICsvReader Create(string path, string tableName, string connectionString, string delimiter = ",", int headerRowCount = 1, string colNames = null, int batchSize = 1000, int timeOut = 300)
        {
            return new CsvReader(path, tableName, connectionString, delimiter, headerRowCount, colNames, batchSize, timeOut);
        }

        public virtual long ImportCsv()
        {
            CreateDataTable();

            if (_error != null)
                throw new Exception("Error creating DataTable: " + _error);

            return ReadCsv();
        }

        /// <summary>
        /// Creates an empty DataTable object from the imported CSV.
        /// </summary>
        /// <param name="colNames"></param>
        protected virtual void CreateDataTable()
        {
            SqlCommand cmd = new SqlCommand
            {
                Connection = new SqlConnection(_connectionString),
                CommandType = CommandType.Text,
            };

            SqlDataAdapter da = new SqlDataAdapter(cmd);

            try
            {
                if (_dataTable == null)
                    _dataTable = new DataTable();

                string select = String.Join(", ", this._colNames);
                cmd.CommandText = string.Format("SELECT TOP 1 {0} FROM {1}", select, _tableName);
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
        /// Read lines from CSV, write to the database one batch at a time. 
        /// </summary>
        protected virtual long ReadCsv()
        {
            int batchCount = 0;
            int batchSize = _batchSize;
            long lineCount = 0;
            long dataRowCount = 0;
            long rowsWritten = 0;
            List<string> dataRows = new List<string>();

            try
            {
                long totalDataRows = File.ReadLines(_path).LongCount() - _headerRowCount;
                long totalBatches = totalDataRows / batchSize;
                long remainder = totalDataRows % batchSize;
                this.TotalDataRows = totalDataRows;

                if (_verbose)
                {
                    Console.WriteLine(string.Format("Total data rows: {0}", totalDataRows));
                    Console.WriteLine(string.Format("Batch size: {0}", batchSize));
                    Console.WriteLine(string.Format("Total batches: {0}", totalBatches));
                    Console.WriteLine(string.Format("Remainder: {0}", remainder));
                }

                foreach (string line in File.ReadLines(_path))
                {
                    lineCount++;

                    // Skip header row(s) if it has one.
                    if (lineCount <= _headerRowCount)
                        continue;

                    dataRows.Add(line);
                    dataRowCount++;

                    // Write data to database if we reach the batch size.
                    long remaining = (totalDataRows - (batchSize*batchCount));

                    if (   
                        (totalDataRows < batchSize && dataRows.Count == totalDataRows) ||
                        (batchCount < totalBatches && dataRows.Count == batchSize) ||
                        (remaining < batchSize && dataRows.Count == remainder)
                    ){
                        
                        batchCount++;
                        rowsWritten += ImportToDatabase(dataRows);

                        if (_verbose)
                        {
                            Console.WriteLine(string.Format("Remaining data rows: {0}", remaining));
                            Console.WriteLine(string.Format("Current batch count: {0}", batchCount));
                            Console.WriteLine(string.Format("Rows written: {0}", rowsWritten));
                        }
                    }
                }
            }
            catch(Exception ex)
            {
                throw ex;
            }
            finally
            {
                dataRows.Clear();
                dataRows = null;
                this.RowsWritten = rowsWritten;
                this.BatchCount = batchCount;
            }

            return dataRowCount;
        }

        /// <summary>
        /// Fill the DataTable object with the CSV data.
        /// </summary>
        /// <param name="colNames"></param>
        /// <param name="dataRows"></param>
        protected virtual void FillDataTable(List<string> dataRows)
        {
            int rowCount = 0;
            if (dataRows == null)
                throw new Exception("Error: argument List<string> dataRows is null");

            if(_dataTable == null)
                throw new Exception("Error: DataTable is null");

            try
            {
                string[] colNames = this._colNames;

                if(colNames == null)
                    throw new Exception("Error: colNames is null");

                foreach (string line in dataRows)
                {
                    if (c.IsEmpty(line))
                        continue;

                    try
                    {
                        DataRow row = _dataTable.NewRow();

                        if (row == null)
                            Console.WriteLine("Error: DataRow is null.");

                        string[] values = c.GetRxCsv(_delimiter).Split(line);

                        if (values == null)
                            Console.WriteLine("Error: DataRow values is null.");

                        if (values.Length == 0)
                            continue;

                        for (int i = 0; i < colNames.Length; i++)
                        {
                            string colName = colNames[i];
                            string val = values[i];
                            DataColumn col = _dataTable.Columns[colName.Replace("[", "").Replace("]", "")];

                            if (col == null)
                                Console.WriteLine("Error: Could not find column name " + colName);

                            WriteValue(val, row, col);
                        }

                        _dataTable.Rows.Add(row);
                    }
                    catch(NullReferenceException ex)
                    {
                        throw ex;
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
            string val = c.GetRxStripQuotes().Replace(value, "").TrimEnd();

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

                case "System.Double":
                case "System.Decimal":
                    decimal? d = c.ConvertToDecimal(val);
                    if (d.HasValue)
                        row[colName] = d.Value;

                    break;

                case "System.Single": //float
                    float? f = c.ConvertToFloat(val);
                    if (f.HasValue)
                        row[colName] = f.Value;

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

                case "System.Char":
                    char? ch = c.ConvertToChar(val);
                    if (ch.HasValue)
                        row[colName] = ch.Value;

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
        protected virtual long ImportToDatabase(List<string> dataRows)
        {
            //Console.WriteLine(string.Format("Writing to database. Timeout = {0}", _timeOut));

            long rowCount = 0;
            SqlConnection conn = new SqlConnection(_connectionString);

            try
            {
                FillDataTable(dataRows);

                if (_verbose)
                {
                    Console.WriteLine(string.Format("Datatable has {0} rows.", _dataTable.Rows.Count));
                    Console.WriteLine(string.Format("Database timeout is {0}.", _timeOut));
                }

                conn.Open();
                var transaction = conn.BeginTransaction();

                using (SqlBulkCopy bulkCopy = new SqlBulkCopy(conn, SqlBulkCopyOptions.Default, transaction))
                {
                    bulkCopy.DestinationTableName = _tableName;
                    bulkCopy.SqlRowsCopied += (sender, eventArgs) => rowCount += eventArgs.RowsCopied;
                    bulkCopy.NotifyAfter = _dataTable.Rows.Count;
                    bulkCopy.BulkCopyTimeout = _timeOut;

                    try
                    {
                        foreach (DataColumn col in _dataTable.Columns)
                            bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);

                        bulkCopy.WriteToServer(_dataTable);
                        transaction.Commit();

                        if(_verbose)
                            Console.WriteLine("Committed database transaction." );
                    }
                    catch (Exception ex)
                    {
                        _error = ex.ToString();
                        Console.WriteLine("Error: " + _error);
                    }
                }
                conn.Close();
            }
            catch (Exception ex)
            {
                _error = ex.ToString();
                Console.WriteLine("Error: " + _error);
                //throw ex;
            }
            finally
            {
                if (conn.State == ConnectionState.Open)
                    conn.Close();

                conn.Dispose();
                conn = null;

                _dataTable.Clear();
                dataRows.Clear(); // clear all data, load next batch.
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
