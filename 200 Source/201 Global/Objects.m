/* 
=========================================================================================
Global - Objects
=========================================================================================
# Script Definition
This script allows for dynamic navigation by connecting the source folder *.m files and the powerbi file.

# User Actions
## In Script
Define your folder and file paths in this script.

## In PowerBI
Define your file path to this file in the `Connections` folder
=========================================================================================
*/

let 
// Storage Definition - If you need to constantly change locations for this system, create mulitple variables for the absolute paths you will be using
cloudMac = "\\Mac\iCloud\Data Analysis\200 Projects\200-002 Bridge Inspection Team Performance Tracking\Bridge-Inspection-Team-Performance-Tracking\",
projectsPC = "C:\Users\psenior\OneDrive - Hardesty & Hanover\Documents\Projects\06223 - NCDOT Structure Management Unit\Bridge-Inspection-Team-Performance-Tracking\",

// Folder Definitions
csv_Raw = "100 Data\102 CSV\01 Raw\",
csv_Cleaned = "100 Data\102 CSV\02 Cleaned\",
csv_Updated = "100 Data\102 CSV\03 Updated\",

Global = "200 Source\201 Global\",
STG = "200 Source\202 Staging\",

// Evaluation Formulas
evalFolder = (folder) => Folder.Files(folder),
evalFile = (file) => Expression.Evaluate(Text.FromBinary(File.Contents(file)),#shared),

gblObjects = (path) =>
[
    // Global Records
    gblFunctions = evalFile(Text.Combine({path, Global,"Functions.m"})),
    gblLists = evalFile(Text.Combine({path, Global,"Lists.m"})),
    gblQuality = evalFile(Text.Combine({path, Global,"Quality.m"})),
    
    // Data Folders
    csv_RawNBI = evalFolder(Text.Combine({path, csv_Raw,"NBI"})),
    csv_RawDOT = evalFolder(Text.Combine({path, csv_Raw,"DOT"})),
    csv_RawCounties = evalFolder(Text.Combine({path, csv_Raw,"Counties"})),
    csv_RawAssignments = evalFolder(Text.Combine({path, csv_Raw,"Assignments"})),

    csv_CleanedStructureData = evalFolder(Text.Combine({path, csv_Cleaned,"Structure Data"})),
    csv_CleanedAssignments = evalFolder(Text.Combine({path, csv_Cleaned,"Assignments"})),
    csv_CleanedAssignedEstimateUpdates = evalFolder(Text.Combine({path, csv_Cleaned,"Estimate Updates"})),
    csv_CleanedAssignedGISUpdates = evalFolder(Text.Combine({path, csv_Cleaned,"GIS Updates"})),

    // Staging Zone
    STG_ETL = evalFile(Text.Combine({path, STG,"01 ETL.m"})),
    STG_DataTables = evalFile(Text.Combine({path, STG,"02 Data Tables.m"})),
    STG_AnalysisTables = evalFile(Text.Combine({path, STG,"03 Analysis Tables.m"})),

    // Queried Data Tables
    csv_dtqAssignedStructures = Table.SelectRows(evalFolder(Text.Combine({path, csv_Cleaned})), each [Name] = "dtqAssignedStructures.csv"),

    // Updated Analysis Tables
    csv_atuAssignedStructures = evalFolder(Text.Combine({path, csv_Updated, "Assigned Structures"}))

]

in 
    if Table.RowCount(Table.SelectRowsWithErrors(Record.ToTable(gblObjects(cloudMac)))) > 0 then gblObjects(projectsPC) else gblObjects(cloudMac)