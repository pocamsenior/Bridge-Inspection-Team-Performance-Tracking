/* 
=========================================================================================
Analysis Tables
=========================================================================================
# Script Definition
This script creates tables that are used for analysis

# Output Tables
1. Assigned Structures
2. Assignments
=========================================================================================
*/

let
// Global Defintions
gblObjects = #shared[gblObjects],
    dtqAssignedStructures = gblObjects[STG_ETL][dtqAssignedStructures],
    dtAssignments = gblObjects[STG_DataTables][dtAssignments],
    atuAssignedStructures = gblObjects[STG_ETL][atuAssignedStructures],


gblFunctions = gblObjects[gblFunctions],
    filterList = gblFunctions[filterList],
    updateDataTypes = gblFunctions[updateDataTypes],

gblLists = gblObjects[gblLists],
    glstDuplicates = gblLists[glstDuplicates],


/* 
=========================================================================================
Assigned Structures
=========================================================================================
*/

tblAssignedStructures =
[

#"Remove Duplicates" = 
    [
        lstColumnNames = Table.ColumnNames(dtqAssignedStructures),
        lstFilteredColumns = List.Combine({List.RemoveFirstN(lstColumnNames),{txt_Index}}),

        txt_Index = "Index",
        txt_StructureInfo = "Structure Info",
        txtKeyColumnName = List.First(filterList(lstColumnNames,{{"number"},{}})),

        compareRow = (rec as record, nlstSelections as list) => 
            let 
                lstSelect = Record.ToList(Record.SelectFields(rec,{txtKeyColumnName,txt_Index})),

                position = List.PositionOf(List.Transform(nlstSelections, each List.ContainsAll(_,lstSelect)),true)
            in
                position,

        lstDuplicateRecords = Table.ToRecords(#"Select Duplicates"),

        #"Group by Structure Number" = Table.SelectRows(Table.Group(dtqAssignedStructures,txtKeyColumnName,{txt_StructureInfo, each Table.AddIndexColumn(_,txt_Index,0,1)}), each Table.RowCount(_[Structure Info]) > 1),

        #"Expand Table Column" = Table.Combine(Table.RemoveColumns(#"Group by Structure Number", txtKeyColumnName)[Structure Info]),

        #"Select Duplicates" = Table.RemoveColumns(Table.SelectRows(#"Expand Table Column", each compareRow(_,glstDuplicates) > -1),txt_Index),

        #"Final Output" = Table.RemoveMatchingRows(dtqAssignedStructures,lstDuplicateRecords)
    ],


#"Add Missing Structures" = 
    [
        tblAssignedStructures = #"Remove Duplicates"[Final Output],

        Assignments_lstColumnNames = Table.ColumnNames(dtAssignments),
        Assignments_lstKeyColumnNames = filterList(Assignments_lstColumnNames,{{"number"},{}}),

        lstAssignedStructureNumbers = dtAssignments[Structure Number],
        lstStructureNumbers = tblAssignedStructures[Structure Number],

        lstCommonStructureNumbers = List.Intersect({lstStructureNumbers,lstAssignedStructureNumbers},Comparer.OrdinalIgnoreCase),
        lstMissingStructureNumbers = List.Difference(lstAssignedStructureNumbers,lstCommonStructureNumbers,Comparer.OrdinalIgnoreCase),


        #"Select Null Rows" = Table.SelectRows(tblAssignedStructures, each [Structure Number] = null),

        #"Select Filled Rows" = Table.SelectRows(tblAssignedStructures, each [Structure Number] <> null),

        #"Replace Nulls with Structure Numbers" =  Table.ReplaceValue(#"Select Null Rows", each [Structure Number], each lstMissingStructureNumbers{Table.PositionOf(#"Select Null Rows",_)}, Replacer.ReplaceValue, Assignments_lstKeyColumnNames),

        #"Final Output" =  Table.Combine({#"Select Filled Rows", #"Replace Nulls with Structure Numbers"})
    ],


#"Add Updated Structures" =
    [

        txt_Index = "Index",
        txt_StructureInfo = "Structure Info",
        txtKeyColumnName = List.First(filterList(lstKeyColumnNames,{{"number"},{}})),
        lstKeyColumnNames = filterList(Table.ColumnNames(atuAssignedStructures),{{"number"},{}}),

        AssignedStructures_lstColumnNames = Table.ColumnNames(tblAssignedStructures),


        tblAssignedStructures = #"Add Missing Structures"[Final Output],

        // May need to update when new NBI Data is posted
        atuAssignedStructures_Latest = Table.SelectColumns(Table.FromRecords(Table.TransformColumns(Table.Group(updateDataTypes(atuAssignedStructures),lstKeyColumnNames,{txt_StructureInfo, each _}), {txt_StructureInfo, each Table.Max(_,"Year Assigned")})[Structure Info]), AssignedStructures_lstColumnNames),


        lstUpdatedRows = Table.ToRecords(Table.SelectColumns(atuAssignedStructures, lstKeyColumnNames)),

        #"Select Updated Rows" = Table.SelectRows(atuAssignedStructures_Latest, each List.Contains(lstUpdatedRows, Record.SelectFields(_,lstKeyColumnNames))),

        #"Select Unchanged Rows" = Table.SelectRows(tblAssignedStructures, each  not(List.Contains(lstUpdatedRows, Record.SelectFields(_,lstKeyColumnNames)))),

        #"Final Output" = Table.Combine({#"Select Unchanged Rows", #"Select Updated Rows"})
    ],


#"Update Data Types" = updateDataTypes(#"Add Updated Structures"[Final Output]),
        
#"Sort by Structure" = Table.Sort(#"Update Data Types",
    {
        {"Structure Number", Order.Ascending}
    }),

#"Final Output" = #"Sort by Structure"

]
    
in

[
    tblAssignedStructures = tblAssignedStructures[Final Output],
    tblAssignments = updateDataTypes(dtAssignments)
]
