using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.IO;

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
        public static bool OutputToCsv(DataTable dataTable, string outputPath, char delimiter = ',')
        {
            StringBuilder csv = new StringBuilder();
            DataColumnCollection cols = dataTable.Columns;
            int columnCount = cols.Count;
            string del = delimiter.ToString();

            for (int i = 0; i < columnCount; i++)
            {
                string colName = cols[i].ColumnName;
                string ending = (i < columnCount-1) ? del : Environment.NewLine;
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

                    string ending = (j < columnCount-1) ? del : Environment.NewLine;
                    csv.AppendFormat("{0}{1}", colName, ending);
                }
            }

            File.WriteAllText(outputPath, csv.ToString());
            return File.Exists(outputPath);
        }

        #region Parsers

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
            int n;

            if (int.TryParse(val, out n))
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
            long n;

            if (long.TryParse(val, out n))
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
            short n;

            if (short.TryParse(val, out n))
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
            decimal d;

            if (decimal.TryParse(val, out d))
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
            bool b;

            if (bool.TryParse(val, out b))
                return b;

            return null;
        }

        /// <summary>
        /// Convert string to DateTime.
        /// </summary>
        /// <param name="val"></param>
        /// <returns></returns>
        public static DateTime? ConvertToDateTime(string val)
        {
            DateTime d;

            if (DateTime.TryParse(val, out d))
                return d;

            return null;
        }

        #endregion Parsers
    }
}
