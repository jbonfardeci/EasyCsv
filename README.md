# EasyCSV

Import and export CSVs the easy way.

Main functionality:

* Reads a CSV file, with optional custom delimiter, into an accessible DataTable object with the original database table schema, and writes to target table.
* Writes a database table into an accessible DataTable object with the original database table schema, and writes to CSV file with optional custom delimiter.

## Usage in C#

Import CSV into a database table.

```C#
string connectionString = "Server=localhost;Database=myDatabaseName;Trusted_Connection=yes;";
string path = @"C:\example.csv";
string tableName = "dbo.TestTable";
char delimiter = ','; // ',' by default
long rowsAdded = 0;
DataTable dataTable = null;

using(var reader = new EasyCsvLib.CsvReader(path, tableName, connectionString, delimiter)){
    dataTable = reader.DataTable; // Just use the datatable for other operations or...
    rowsAdded = reader.ImportCsv(); // ...import into the database table.
}

bool success = rowsAdded > 0;
```

Export results of a SQL query to CSV.

```C#
string connectionString = "Server=localhost;Database=myDatabaseName;Trusted_Connection=yes;";
string queryString = "SELECT * FROM dbo.TestTable";
char delimiter = ','; // ',' by default
bool success = false;
DataTable dataTable = null;

using(var writer = new EasyCsvLib.CsvWriter(path, connectionString, queryString, delimiter)){
    dataTable = writer.DataTable; // Just use the datatable for other operations or...
    success = writer.OutputToCsv(); // ...export to CSV.
}
```

## Usage in PowerShell

Import CSV into a database table.

```PowerShell
[System.Reflection.Assembly]::LoadFrom(@"
C:\bin\EasyCsvLib.dll
"@);

$inputDir = @"
C:\csv\{0}
"@;

$connectionString = "Server=localhost;Database=myDataTable;Trusted_Connection=yes;";

function importCsv($path, $tableName, $connectionString){
    $rowsAdded = 0;
    $reader = New-Object EasyCsvLib.CsvReader($path, $tableName, $connectionString);
    $rowsAdded = $reader.ImportCsv();
    $reader.Dispose();

    return $rowsAdded;
}

$path = [string]::Format($inputDir, "example.csv");
$tableName = "dbo.TestTable";
$rowCount = importCsv $path $tableName $connectionString;
echo "Added $rowCount rows.";
```

Export table to CSV.

```PowerShell
[System.Reflection.Assembly]::LoadFrom(@"
C:\bin\EasyCsvLib.dll
"@);

$outputDir = @"
C:\csv\{0}
"@;

$connectionString = "Server=localhost;Database=myDataTable;Trusted_Connection=yes;";

function exportCsv($path, $connectionString, $queryString){
    $success = $false;
    $writer = New-Object EasyCsvLib.CsvWriter($path, $connectionString, $queryString);
    $success = $writer.OutputToCsv();
    $writer.Dispose();

    return $success;
}

$path = [string]::Format($outputDir, "example.csv");
$queryString = "SELECT * FROM dbo.TestTable";
$success = exportCsv $path $connectionString $tasksQuery;
echo "Export Tasks: $success";
```

## MIT License

Copyright 2018 John Bonfardeci

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.