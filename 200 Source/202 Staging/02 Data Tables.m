/* 
=========================================================================================
Staging - Data Tables
=========================================================================================
# Script Definition
This script creates data tables that are used create analysis tables

# Output Tables
1. Structure Data
2. Assignments
=========================================================================================
*/

let
// Global Defintions
gblObjects = #shared[gblObjects],
    NBI = gblObjects[STG_ETL][NBI],
    DOT = gblObjects[STG_ETL][DOT],
    Assignments = gblObjects[STG_ETL][Assignments],

gblFunctions = gblObjects[gblFunctions],
    filterList = gblFunctions[filterList],
    reorderColumns = gblFunctions[reorderColumns],
    updateDataTypes = gblFunctions[updateDataTypes],

/* 
=========================================================================================
Structure Data
=========================================================================================
*/

// NBI
NBI_lstColumnNames = Table.ColumnNames(NBI),
NBI_txtKeyColumnName = List.First(filterList(NBI_lstColumnNames,{{"structure"},{}})),

// DOT
DOT_lstColumnsNames = Table.ColumnNames(DOT),
DOT_txtKeyColumnName = List.First(filterList(DOT_lstColumnsNames,{{"bridge"},{}})),

dtStructureData =
[
    #"Join NBI and DOT" = Table.RemoveColumns(Table.Join(NBI,NBI_txtKeyColumnName,DOT,DOT_txtKeyColumnName,JoinKind.RightOuter),NBI_txtKeyColumnName),

    #"Rename Columns" =
        let
            lstColumnNames = Table.ColumnNames(#"Join NBI and DOT"),

            nlstColumnNames_Split = List.Transform(lstColumnNames, each Text.Split(Text.Proper(Text.Replace(_,"_"," "))," ")),

            nlstColumnNames_Updated = List.Transform(nlstColumnNames_Split, each List.RemoveNulls(List.Transform(_, (ListItem) => 
                if Text.Contains(ListItem,"Bridge", Comparer.OrdinalIgnoreCase) then "Structure" else ListItem))),

            lstNewColumnNames = List.Transform(nlstColumnNames_Updated, each Text.Combine(_," ")),
            nlstTransformations_Rename = List.Zip({lstColumnNames, lstNewColumnNames})

        in

            Table.RenameColumns(#"Join NBI and DOT", nlstTransformations_Rename),

    #"Reorder Columns" = reorderColumns(#"Rename Columns",{0.3,1,2,3,4,5,6,7,8,0,0.1,0.4,0.5,13,14,15,16,17,19,18}),

    #"Final Output" = updateDataTypes(#"Reorder Columns")
]
    
in

[
    dtStructureData = dtStructureData[Final Output],
    dtAssignments = updateDataTypes(Assignments)
]
