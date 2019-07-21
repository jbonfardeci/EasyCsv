# EasyCSV

Import and export CSVs the easy way.

Main functionality:

* Reads a CSV file, mapping values to correct data types. Streams batches of CSV data rows to target table via SqlBulkCopy with a transaction.
* Writes ad-hoc or stored procedure query results into an accessible DataTable object. Writes to CSV file.

## Usage in C#

Import CSV into a database table.

```C#
string connectionString = "Server=localhost;Database=myDatabaseName;Trusted_Connection=yes;";
string path = @"C:\example.csv";
string tableName = "dbo.TestTable";
string delimiter = ','; // ',' by default
int headerRowCount = 1; // The number of header rows to skip. Set to 1 whether you use your own column names or the header in the CSV.
string colNames = null; // You can use the column names in the CSV header or provide your own comma-delimited column names. They number and order must match the columns in the CSV.
int batchSize = 1000; // Improt large (multi-GB) files in batch sizes of your choosing, streams in small chunks to the database.
int timeOut = 300; // Set the database conneciton timeout. Increase for large files. Default is 5 minutes.
long rowsWritten = 0;
long totalDataRows = 0;

using(ICsvReader reader = EasyCsvLib.CsvReader.Create(path, tableName, connectionString, delimiter, headerRowCount, colNames, batchSize, timeOut)){
    reader.Verbose = false;
    reader.ImportCsv(); // ...import into the database table.
    totalDataRows =  reader.TotalDataRows;
    rowsWritten = reader.RowsWritten;
}

bool success = rowsWritten == totalDataRows;
```

Export results of a SQL query to CSV.

```C#
string connectionString = "Server=localhost;Database=myDatabaseName;Trusted_Connection=yes;";
string queryString = "SELECT * FROM dbo.TestTable";
char delimiter = ','; // ',' by default
bool success = false;
DataTable dataTable = null;

using(ICsvWriter writer = EasyCsvLib.CsvWriter.Create(path: path, connectionString: connectionString, queryString: queryString, delimiter: ",", isStoredProcedure: false)){
    dataTable = writer.DataTable; // Just use the datatable for other operations or...
    success = writer.OutputToCsv(); // ...export to CSV.
}
```

## Usage in PowerShell

Import CSV into a database table.

```PowerShell
[System.Reflection.Assembly]::LoadFrom("C:\example\bin\EasyCsvLib.dll");

$inputDir = "C:\example\csv\{0}";
$connectionString = "Server=localhost;Database=myDataTable;Trusted_Connection=yes;";

function importCsv($path, $tableName, $delimiter = ",", $headerRowCount = 1, $colNames = $null, $batchSize = 1000, $timeOut = 300){
    $csv = Test-Path -LiteralPath $path -ErrorAction Stop
    $success = $false;

    $reader = New-Object EasyCsvLib.CsvReader($path, $tableName, $connectionString, $delimiter, $headerRowCount, $colNames, $batchSize, $timeOut);
    $reader.Verbose = $true;

    $reader.ImportCsv();

    $rowsAdded = $reader.RowsWritten;
    $totalRows = $reader.TotalDataRows;
    $batchCount = $reader.BatchCount;

    if($rowsAdded -eq $totalRows) {
       $success = $true;
       Write-Host "Success! All $totalRows rows were imported to the database.";
    }
    else {
       Write-Error "Error! Only $rowsAdded of $totalRows rows were imported to the database.";
    }

    $reader.Dispose();

    return $success;
}

$path = [string]::Format($inputDir, "example.csv");
$tableName = "dbo.TestTable";
importCsv $path $tableName;
```

Export table to CSV.

```PowerShell
[System.Reflection.Assembly]::LoadFrom("C:\example\bin\EasyCsvLib.dll");

$outputDir = "C:\example\csv\{0}";
$connectionString = "Server=localhost;Database=myDataTable;Trusted_Connection=yes;";

function exportCsv($path, $connectionString, $queryString, $delimiter, $isProc){
    $success = $false;
    $writer = New-Object EasyCsvLib.CsvWriter($path, $connectionString, $queryString, $delimiter, $isProc);
    $success = $writer.OutputToCsv();
    $writer.Dispose();

    return $success;
}

$path = [string]::Format($outputDir, "example.csv");
$queryString = "SELECT * FROM dbo.TestTable";
$success = exportCsv $path $connectionString $tasksQuery "," $false;
echo "Export Tasks: $success";
```

## MIT License

Copyright 2018 John Bonfardeci

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.