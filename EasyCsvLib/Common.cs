using System;
using System.Text;
using System.Data;
using System.IO;
using System.Text.RegularExpressions;
using System.Collections.Generic;
using System.Linq;

namespace EasyCsvLib
{
    public static class Common
    {
        /// <summary>
        /// Output the DataTable to a CSV. 
        /// </summary>
        /// <param name="dataTable"></param>
        /// <param name="outputPath"></param>
        /// <param name="delimiter"></param>
        /// <returns></returns>
        public static bool OutputToCsv(DataTable dataTable, string outputPath, string delimiter = ",")
        {
            string csv = DataTableToCsv(dataTable, delimiter);
            File.WriteAllText(outputPath, csv);
            return File.Exists(outputPath);
        }

        public static string DataTableToCsv(DataTable dataTable, string delimiter = ",")
        {
            StringBuilder sb = new StringBuilder();
            DataColumnCollection cols = dataTable.Columns;
            int columnCount = cols.Count;

            for (int i = 0; i < columnCount; i++)
            {
                string colName = cols[i].ColumnName;
                string ending = (i < columnCount - 1) ? delimiter : Environment.NewLine;
                sb.AppendFormat("{0}{1}", colName, ending);
            }

            foreach (DataRow row in dataTable.Rows)
            {
                for (int j = 0; j < columnCount; j++)
                {
                    string colName = cols[j].ColumnName;
                    string value = row[colName].ToString();
                    string type = cols[colName].DataType.FullName;
                    string val = null;

                    if (IsEmpty(value))
                        val = null;
                    else if (type == "System.String")
                        val = string.Format("\"{0}\"", value);
                    else if (type == "System.DateTime")
                        val = ((DateTime)row[colName]).ToString();
                    else
                        val = value.ToString();

                    string ending = (j < columnCount - 1) ? delimiter : Environment.NewLine;
                    sb.AppendFormat("{0}{1}", val, ending);
                }
            }

            return sb.ToString();
        }

        public static string[] GetColNamesFromCsv(string path, string delimiter)
        {
            string line = File.ReadLines(path).FirstOrDefault();
            return SplitColNames(line, delimiter);
        }

        /// <summary>
        /// Gets the column names from the header in the CSV file.
        /// </summary>
        /// <param name="lines"></param>
        /// <returns></returns>
        public static string[] SplitColNames(string colNames, string delimiter)
        {
            var _columnNames = new List<string>();

            string[] _colNames = Common.GetRxCsv(delimiter).Split(colNames);

            foreach(string colName in _colNames)
            {
                string col = Common.GetRxStripQuotes().Replace(colName, "").Trim();

                if (!col.StartsWith("["))
                    _columnNames.Add("[" + col + "]");
                else
                    _columnNames.Add(col);
            }

            return _columnNames.ToArray();
        }

        private static Regex _rxCsv = null;
        public static Regex GetRxCsv(string delimiter)
        {
            if (_rxCsv == null)
                _rxCsv = new Regex(delimiter + "(?=(?:[^\"]*\"[^\"]*\")*(?![^\"]*\"))", RegexOptions.Multiline);

            return _rxCsv;
        }

        private static Regex _rxStripQuotes = null;
        public static Regex GetRxStripQuotes()
        {
            if (_rxStripQuotes == null)
                _rxStripQuotes = new Regex("(^\"|\"$)");

            return _rxStripQuotes;
        }

        /// <summary>
        /// Create a DDL statement to create a table from an array of column names. 
        /// </summary>
        /// <param name="colNames"></param>
        /// <param name="defaultSqlDataType"></param>
        /// <returns></returns>
        public static string CreateTableDdl(string tableName, string[] colNames, string[] sqlTypes, string schema = "dbo")
        {
            int columnCount = colNames.Length;
            string nl = Environment.NewLine;
            var sb = new StringBuilder();

            sb.AppendFormat("DROP TABLE IF EXISTS [{0}].[{1}];{2}{2}", schema, tableName, nl);

            sb.AppendFormat("CREATE TABLE [{0}].[{1}]({2}", schema, tableName, nl);

            int i = 0;

            foreach(string colName in colNames)
            {
                string ending = (i < columnCount - 1) ? string.Concat(",", nl) : nl;
                sb.AppendFormat("    {0} {1} NULL{2}", string.Concat("[", colName, "]"), sqlTypes[i], ending);

                i++;
            }

            sb.AppendFormat(");{0}{0}", nl);

            return sb.ToString();
        }

        /// <summary>
        /// Gets a collection of files from a provided directory path.
        /// </summary>
        /// <param name="path"></param>
        /// <param name="extensionFilter"></param>
        /// <param name="folderless"></param>
        public static List<String> ReadFiles(string path, string extensionFilter = "csv", bool includeSubfolders = false)
        {
            if (!Directory.Exists(path))
                throw new Exception("The provided directory path does not exist.");

            List<String> files = new List<String>();

            try
            {
                string searchPattern = Common.IsEmpty(extensionFilter) ? null 
                    : extensionFilter.StartsWith("*.") ? extensionFilter 
                    : string.Concat("*.", extensionFilter);

                foreach (string f in Directory.GetFiles(path, searchPattern))
                    files.Add(f);

                if (includeSubfolders)
                {
                    foreach (string d in Directory.GetDirectories(path))
                        files.AddRange(ReadFiles(d, extensionFilter, includeSubfolders));
                }
                
            }
            catch (Exception ex)
            {
                throw ex;
            }

            return files;
        }

        #region Parsers

        public static Regex RxNumeric = new Regex("[^0-9\\.]", RegexOptions.Compiled);
        public static Regex RxBoolTrue = new Regex("(true|yes|1)", RegexOptions.IgnoreCase);
        public static Regex RxBoolFalse = new Regex("(false|no|0)", RegexOptions.IgnoreCase);
        public static Regex RxDateTime = new Regex("[^0-9\\-\\/]", RegexOptions.IgnoreCase);
        public static Regex RxNull = new Regex("^(\\(null\\)|null)$", RegexOptions.IgnoreCase);

        /// <summary>
        /// Check if value is empty.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static bool IsEmpty(string value)
        {
            return string.IsNullOrEmpty(value) || string.IsNullOrWhiteSpace(value) || RxNull.IsMatch(value);
        }

        public static char? ConvertToChar(string val)
        {
            if (IsEmpty(val))
                return null;

            char c;

            if (char.TryParse(val, out c))
                return c;

            return null;
        }

        public static float? ConvertToFloat(string val)
        {
            if (IsEmpty(val))
                return null;

            decimal d;

            if (decimal.TryParse(RxNumeric.Replace(val, ""), out d))
                return float.Parse(d.ToString());

            return null;
        }

        /// <summary>
        /// Convert string to Int32 (int).
        /// </summary>
        /// <param name="val"></param>
        /// <returns></returns>
        public static int? ConvertToInt(string val)
        {
            if (IsEmpty(val))
                return null;

            decimal d;

            if (decimal.TryParse(RxNumeric.Replace(val, ""), out d))
                return Convert.ToInt32(d);

            return null;
        }

        /// <summary>
        /// Convert string to Int64 (long).
        /// </summary>
        /// <param name="val"></param>
        /// <returns></returns>
        public static long? ConvertToInt64(string val)
        {
            if (IsEmpty(val))
                return null;

            decimal d;

            if (decimal.TryParse(RxNumeric.Replace(val, ""), out d))
                return Convert.ToInt64(d);

            return null;
        }

        /// <summary>
        /// Convert string to Int16 (short).
        /// </summary>
        /// <param name="val"></param>
        /// <returns></returns>
        public static short? ConvertToInt16(string val)
        {
            if (IsEmpty(val))
                return null;

            decimal d;

            if (decimal.TryParse(RxNumeric.Replace(val, ""), out d))
                return Convert.ToInt16(d);

            return null;
        }

        /// <summary>
        /// Convert string to decimal.
        /// </summary>
        /// <param name="val"></param>
        /// <returns></returns>
        public static decimal? ConvertToDecimal(string val)
        {
            if (IsEmpty(val))
                return null;

            decimal d;

            if (decimal.TryParse(RxNumeric.Replace(val, ""), out d))
                return d;

            return null;
        }

        /// <summary>
        /// Converyt string to Boolean.
        /// </summary>
        /// <param name="val"></param>
        /// <returns></returns>
        public static bool? ConvertToBoolean(string val)
        {
            if (IsEmpty(val))
                return null;

            if (RxBoolTrue.IsMatch(val))
                return true;

            if (RxBoolFalse.IsMatch(val))
                return false;

            return false;
        }

        /// <summary>
        /// Convert string to DateTime.
        /// </summary>
        /// <param name="val"></param>
        /// <returns></returns>
        public static DateTime? ConvertToDateTime(string val)
        {
            if (IsEmpty(val))
                return null;

            DateTime d;

            if (DateTime.TryParse(RxDateTime.Replace(val, ""), out d))
                return d;

            return null;
        }

        #endregion Parsers
    
    }
}
