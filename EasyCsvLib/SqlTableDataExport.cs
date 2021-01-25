using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Data.SqlClient;
using c = EasyCsvLib.Common;
using System.IO;

namespace EasyCsvLib
{
    public class SqlTableDataExport
    {
        #region class vars
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

        private string _outputDir = null;
        public string OutputDir
        {
            get
            {
                return _outputDir;
            }
            set
            {
                _outputDir = value;
            }
        }
        #endregion

        public SqlTableDataExport(string connectionString, string outputDir)
        {
            this._connectionString = connectionString;
            this._outputDir = outputDir;
        }

        /// <summary>
        /// Export data to an SQL MERGE file.
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="schema"></param>
        public bool ExportDataToSql(string tableName, string matchColumns, string identityColumn = null, bool includeIdentityColumn = false, string schema = "dbo")
        {
            DataTable dt = GetTableData(tableName, schema);

            try
            {
                string strColumns = GetStringColumns(dt);
                string nl = Environment.NewLine;
                var sb = new StringBuilder();

                // MERGE
                sb.AppendFormat("MERGE INTO [{0}].[{1}] AS t{2}", schema, tableName, nl);
                sb.AppendFormat("\tUSING ({0}", nl);
                sb.AppendFormat("\t\tVALUES{0}", nl);

                // rows
                sb.Append(this.DataRowsToSqlStatement(dt, identityColumn, includeIdentityColumn));

                sb.Append(nl);
                sb.AppendFormat(") AS x ({0}){1}", strColumns, nl);

                // ON
                sb.Append("\tON ");

                string[] matchCols;
                if (matchColumns.Contains(","))
                    matchCols = matchColumns.Split(',');
                else
                    matchCols = new string[] { matchColumns };
                
                for (int i = 0; i < matchCols.Length; i++)
                {
                    string col = matchCols[i].Trim();
                    sb.AppendFormat("t.[{0}] = x.[{0}]", col);

                    if (i < matchCols.Length - 1)
                        sb.Append(" AND ");
                    else
                        sb.Append(nl);
                }

                // UPDATE
                sb.AppendFormat("{0}WHEN MATCHED THEN{0}", nl);
                sb.AppendFormat("\tUPDATE SET{0}", nl);

                for (int i = 0; i < dt.Columns.Count; i++)
                {
                    var col = dt.Columns[i].ColumnName;
                    sb.AppendFormat("\t\tt.[{0}] = x.[{0}]", col);

                    if (i < dt.Columns.Count - 1)
                        sb.AppendFormat(",{0}", nl);
                    else
                        sb.Append(nl);
                }

                // INSERT
                sb.AppendFormat("WHEN NOT MATCHED THEN{0}", nl);
                sb.AppendFormat("\tINSERT({0}){1}", strColumns, nl);
                sb.AppendFormat("\tVALUES({0});", strColumns);

                if (!Directory.Exists(this.OutputDir))
                    Directory.CreateDirectory(this.OutputDir);

                File.WriteAllText(string.Concat(this.OutputDir, tableName, ".sql"), sb.ToString());
                return true;
            }
            catch(Exception ex)
            {
                throw ex;
            }
            finally
            {
                dt.Dispose();
            }
        }

        protected string GetStringColumns(DataTable dt)
        {
            var sb = new StringBuilder();

            for(int i=0; i < dt.Columns.Count; i++)
            {
                var col = dt.Columns[i];
                sb.AppendFormat("[{0}]", col.ColumnName);

                if (i < dt.Columns.Count - 1)
                    sb.Append(", ");
            }

            return sb.ToString();
        }

        protected DataTable GetTableData(string tableName, string schema = "dbo")
        {
            SqlCommand cmd = new SqlCommand()
            {
                Connection = new SqlConnection(_connectionString),
                CommandType = CommandType.Text,
                CommandText = string.Format("SELECT * FROM [{0}].[{1}]", schema, tableName)
            };

            SqlDataAdapter da = new SqlDataAdapter(cmd);
            DataTable dt = new DataTable();

            try
            {
                cmd.Connection.Open();
                da.Fill(dt);
                cmd.Connection.Close();
            }
            catch (Exception ex)
            {
                throw ex;
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

            return dt;
        }

        protected string DataRowsToSqlStatement(DataTable dt, string identityColumn = null, bool includeIdentityColumn = false)
        {
            StringBuilder sb = new StringBuilder();
            DataColumnCollection cols = dt.Columns;
            int columnCount = cols.Count;

            for(int i=0; i < dt.Rows.Count; i++)
            {
                DataRow row = dt.Rows[i];

                sb.Append("(");

                for (int j = 0; j < columnCount; j++)
                {
                    string colName = cols[j].ColumnName;

                    if (!c.IsEmpty(identityColumn) && colName == identityColumn && !includeIdentityColumn)
                        continue;

                    string value = row[colName].ToString();
                    string type = cols[colName].DataType.FullName;
                    string val = null;

                    if (c.IsEmpty(value))
                        val = "NULL";
                    else if (type == "System.String")
                        val = string.Format("'{0}'", value.Replace("'", "''"));
                    else if (type == "System.DateTime")
                        val = "'" + ((DateTime)row[colName]).ToString("o") + "'";
                    else if (type == "System.Boolean")
                        val = ((bool)row[colName]) ? "1" : "0";
                    else
                        val = value.ToString();

                    sb.Append(val);

                    if (j < columnCount - 1)
                        sb.Append(", ");
                }

                sb.Append(")");

                if (i < dt.Rows.Count - 1)
                    sb.AppendFormat(",{0}", Environment.NewLine);
            }

            return sb.ToString();
        }
    }
}
