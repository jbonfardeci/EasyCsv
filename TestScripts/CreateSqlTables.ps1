[System.Reflection.Assembly]::LoadFrom("C:\Users\jbonfardeci\source\repos\EasyCsv\EasyCsvLib\bin\Debug\EasyCsvLib.dll");

$outputPath = "C:\Users\jbonfardeci\source\repos\EasyCsv\TestScripts\sql\mahso_tables.sql";
$inputFolderPath = "C:\Users\jbonfardeci\source\repos\mahso-db\CsvToDdl";

function buildTables($inputPath, $outputPath){
    $success = $false;
    $writer = New-Object EasyCsvLib.SqlTableBuilder($inputPath);
    $success = $writer.BuildTableSql($outputPath);
    $writer.Dispose();
    return $success;
}

$success = buildTables $inputFolderPath $outputPath;
echo "Build success: $success";