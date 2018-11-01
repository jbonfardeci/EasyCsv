using System;
using NUnit.Framework;
using EasyCsvLib;

namespace EasyCsvLibTests
{
    public class Tests
    {

        private string _connectionString = "Server=localhost;Database=myDatabaseName;Trusted_Connection=yes;";
        private string _root = @"C:\";

        [Test]
        public void ImportCsv()
        {
            ICsvReader csv = CsvReader.Create(path: _root+"example.csv", tableName: "TestTable", connectionString: _connectionString);
            long rowsAdded = csv.ImportCsv();
            int rowCount = csv.DataTable.Rows.Count;
            string error = csv.Error;
            csv.Dispose();

            if (error != null)
                throw new Exception(error);

            Assert.IsTrue(rowsAdded == rowCount);
        }

        [Test]
        public void OutputToCsv()
        {
            string outputPath = _root + "output_test.csv";
            var csv = CsvReader.Create(path: _root + "example.csv", tableName: "TestTable", connectionString: _connectionString);
            bool success = csv.OutputToCsv(outputPath);
            string error = csv.Error;
            csv.Dispose();

            if (error != null)
                throw new Exception(error);

            Assert.IsTrue(success);
        }

        [Test]
        public void OutputTableDef()
        {
            string outputPath = _root + "output_test_definition.txt";
            ICsvReader csv = CsvReader.Create(path: _root + "example.csv", tableName: "TestTable", connectionString: _connectionString);
            bool success = csv.OutputTableDefinition(outputPath);
            string error = csv.Error;
            csv.Dispose();

            if (error != null)
                throw new Exception(error);

            Assert.IsTrue(success);
        }

        [Test]
        public void OutputQueryToCsv()
        {
            ICsvWriter csv = CsvWriter.Create(path: _root + "output_from_table.csv", queryString: "SELECT * FROM dbo.TestTable", connectionString: _connectionString);
            bool success = csv.OutputToCsv();
            string error = csv.Error;
            csv.Dispose();

            if (error != null)
                throw new Exception(error);

            Assert.IsTrue(success);
        }
    }
}
