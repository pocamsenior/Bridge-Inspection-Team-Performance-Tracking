/* 
=========================================================================================
Global - Quality
=========================================================================================
# Script Definition
This script contains Quality Checks for  tables throughout the source folder.
=========================================================================================
*/

let
// Global Defintions
gblObjects = #shared[gblObjects],
    tblAssignedStructures = gblObjects[STG_DataTables][tblAssignedStructures],

gblFunctions = gblObjects[gblFunctions],
    filterList = gblFunctions[filterList],

Duplicates_tblAssignedStructures = 
[
    DuplicateCount = List.Count(AllDuplicates),
    PendingDuplicateCount = List.Count(PendingDuplicates),

    AllDuplicates = tblAssignedStructures[Remove Duplicates From Assignments][Group by Structure Number][Structure Number],
    RecordedDuplicates = tblAssignedStructures[Remove Duplicates From Assignments][Select Duplicates][Structure Number],

    PendingDuplicates = List.RemoveNulls(List.Difference(AllDuplicates,RecordedDuplicates))
],

MissingStructures_tblAssignedStructures =
[
    MissingStructureCount = Table.RowCount(tblAssignedStructures[Add Missing Structures][Create Table of Missing Structures]),

    tblMissingStructures = 
        let
            lstColumnNames = Table.ColumnNames(tblAssignedStructures[Add Missing Structures][Create Table of Missing Structures]),
            lstColumnFilters = {{"Number","Assigned","Assignment"},{}},
            lstColumns_Filtered = filterList(lstColumnNames,lstColumnFilters)
        in    
            Table.SelectColumns(tblAssignedStructures[Add Missing Structures][Create Table of Missing Structures],lstColumns_Filtered)
]

in

[
    Duplicates_tblAssignedStructures = Duplicates_tblAssignedStructures,
    MissingStructures_tblAssignedStructures = MissingStructures_tblAssignedStructures
]