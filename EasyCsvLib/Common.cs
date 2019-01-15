using System;
using System.Text;
using System.Data;
using System.IO;
using System.Text.RegularExpressions;

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
            StringBuilder csv = new StringBuilder();
            DataColumnCollection cols = dataTable.Columns;
            int columnCount = cols.Count;

            for (int i = 0; i < columnCount; i++)
            {
                string colName = cols[i].ColumnName;
                string ending = (i < columnCount-1) ? delimiter : Environment.NewLine;
                csv.AppendFormat("{0}{1}", colName, ending);
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

                    string ending = (j < columnCount-1) ? delimiter : Environment.NewLine;
                    csv.AppendFormat("{0}{1}", val, ending);
                }
            }

            File.WriteAllText(outputPath, csv.ToString());
            return File.Exists(outputPath);
        }

        #region Parsers

        public static Regex RxInt = new Regex("[^0-9]", RegexOptions.Compiled);
        public static Regex RxDecimal = new Regex("[^0-9\\.]", RegexOptions.Compiled);
        public static Regex RxBoolTrue = new Regex("(true|yes|1)", RegexOptions.IgnoreCase);
        public static Regex RxBoolFalse = new Regex("(false|no|0)", RegexOptions.IgnoreCase);
        public static Regex RxDateTime = new Regex("[^0-9\\-\\/]", RegexOptions.IgnoreCase);

        /// <summary>
        /// Check if value is empty.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static bool IsEmpty(string value)
        {
            return string.IsNullOrEmpty(value) || string.IsNullOrWhiteSpace(value);
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

            int n;

            if (int.TryParse(RxInt.Replace(val, ""), out n))
                return n;

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

            long n;

            if (long.TryParse(RxInt.Replace(val, ""), out n))
                return n;

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

            short n;

            if (short.TryParse(RxInt.Replace(val, ""), out n))
                return n;

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

            if (decimal.TryParse(RxDecimal.Replace(val, ""), out d))
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
