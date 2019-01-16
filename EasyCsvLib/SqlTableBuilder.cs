using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using c = EasyCsvLib.Common;

namespace EasyCsvLib
{
    public class SqlTableBuilder: IDisposable
    {
        public List<string> files { get; set; }
        public string Delimiter { get; set; }
        public bool IncludeSubfolders { get; set; }

        public SqlTableBuilder(string folderInputPath, string extensionFilter = "csv", string delimiter = ",", bool includeSubfolders = false)
        {
            this.files = c.ReadFiles(folderInputPath, extensionFilter);
            this.Delimiter = delimiter;
            this.IncludeSubfolders = includeSubfolders;
        }

        public bool BuildTableSql(string outputPath, string schema = "dbo", string defaultSqlType = "nvarchar(255)")
        {
            var tableDefinitions = new StringBuilder();
            Regex rxExt = new Regex("(.|)\\w+$");

            try
            {
                foreach (string file in this.files)
                {
                    string[] lines = File.ReadAllLines(file);
                    string[] colNames = c.GetColNames(lines, this.Delimiter);
                    string tableName = Path.GetFileName(rxExt.Replace(file, ""));
                    string ddl = c.CreateTableDdl(tableName: tableName, colNames: colNames, schema: schema, defaultSqlDataType: defaultSqlType);
                    tableDefinitions.Append(ddl);
                }
            }
            catch(Exception ex)
            {
                throw ex;
            }

            File.WriteAllText(outputPath, tableDefinitions.ToString());
            return File.Exists(outputPath);
        }

        #region IDisposable Support
        private bool disposedValue = false; // To detect redundant calls

        protected virtual void Dispose(bool disposing)
        {
            if (!disposedValue)
            {
                if (disposing)
                {
                    // TODO: dispose managed state (managed objects).
                }

                // TODO: free unmanaged resources (unmanaged objects) and override a finalizer below.
                // TODO: set large fields to null.
                this.files = null;
                this.Delimiter = null;

                disposedValue = true;
            }
        }

        // TODO: override a finalizer only if Dispose(bool disposing) above has code to free unmanaged resources.
        ~SqlTableBuilder()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(false);
        }

        // This code added to correctly implement the disposable pattern.
        public void Dispose()
        {
            // Do not change this code. Put cleanup code in Dispose(bool disposing) above.
            Dispose(true);
            // TODO: uncomment the following line if the finalizer is overridden above.
            GC.SuppressFinalize(this);
        }

        #endregion
    }
}
