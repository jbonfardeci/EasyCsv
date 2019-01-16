# change the paths to your environment.
[System.Reflection.Assembly]::LoadFrom("C:\Users\jbonfardeci\source\repos\EasyCsv\TestScripts\bin\EasyCsvLib.dll");

$delimiter = ",";

$schema = "dbo";

$extension = "csv";

$includeSubFolders = $false;

$sqlOutputPath = "C:\Users\jbonfardeci\source\repos\EasyCsv\TestScripts\sql\mahso_tables.sql";

$csvSourceFolder = "C:\Users\jbonfardeci\source\repos\mahso-db\CsvToDdl";

$connectionString = "Data Source=localhost; Initial Catalog=RealPropertySystemDev; Trusted_Connection=True;";

# Generate a DDL SQL script with create tabel statements.
function buildTables($inputPath, $sqlOutputPath, $ext, $delimiter, $includeSubfolders, $schema, $defaultSqlType){
    $success = $false;
    $writer = New-Object EasyCsvLib.SqlTableBuilder($inputPath, $ext, $delimiter, $includeSubfolders);
    $success = $writer.BuildTableSql($sqlOutputPath);
    $writer.Dispose();
    return $success;
}

# Execute SQL DDL
function executeSql($path){
    $exec = New-Object EasyCsvLib.Executioner;
    $rows = $exec.ExecuteQuery($connectionString, $path);
    return $rows
}

# Import CSV file
function importCsv($path, $tableName, $delimiter){
    $rowsAdded = 0;
    $reader = New-Object EasyCsvLib.CsvReader($path, $tableName, $connectionString, $delimiter);
    $rowsAdded = $reader.ImportCsv();
    $reader.Dispose();

    return $rowsAdded;
}


# Build the DDL script.
$success = buildTables $csvSourceFolder $sqlOutputPath $extension $delimiter $includeSubFolders $schema "nvarchar(255)";
echo "Build success: $success";


# Run the DDL script.
if($success){
    $success = executeSql $sqlOutputPath;
    echo "Executed DDL: $success";
}


#Import the CSVs
$files = Get-ChildItem $csvSourceFolder;

for($i = 0; $i -lt $files.Count; $i++){
    $file = $files[$i];
    $filePath = $file.FullName;
    $tableName = $file.Name -replace "(.|)\w+$", "";
    
    echo $tableName;
    $success = importCsv $filePath $tableName $delimiter;
}