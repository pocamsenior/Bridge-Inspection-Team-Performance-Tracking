/* 
=========================================================================================
Global - Functions
=========================================================================================
# Script Definition
This script contains functions that can be used throughout the project source folder.
=========================================================================================
*/

let
// Global Defintions
gblObjects = #shared[gblObjects],

gblLists = gblObjects[gblLists],
    glstDataTypes = gblLists[glstDataTypes],


gblFunctions =
[

/* 
=========================================================================================
Import Data
=========================================================================================
*/

// Creates a record of tables from a record of binary files
importFiles = (files as record) =>
    let
        // creates a table of binary files
        createTables = (file) =>
            let
                #"Filter Out Hidden Files" = Table.SelectRows(file, each not Text.StartsWith([Name],".")),

                #"Transform Binary to Table" = Table.TransformColumns(#"Filter Out Hidden Files",{"Content", each Table.PromoteHeaders(Csv.Document(_))}),

                #"Add Refresh Timestamp Column" = Table.AddColumn(#"Transform Binary to Table", "Refresh Timestamp",each DateTime.LocalNow()),

                #"Sort by Name" = Table.Sort(#"Add Refresh Timestamp Column",{"Name",Order.Ascending})
            in
                #"Sort by Name",

        lstRecordFieldNames = Record.FieldNames(files),
        lstTables = List.Transform(Record.ToList(files), each createTables(_))
    in
        Record.FromList(lstTables,lstRecordFieldNames),

/* 
=========================================================================================
Enhance Data
=========================================================================================
*/

// Creates a record of tables that have been enhanced with functions tailor to each table using. Helpful if you need to add file attributes to the data table
enhanceTables = (recs as record, lstFunctions as list) =>
    let
        lstRecordFieldNames = Record.FieldNames(recs),
        lstTables = Record.ToList(recs),

        #"Enhance Tables" = List.Transform(lstTables, each 
            let
                Position = List.PositionOf(lstTables,_)
            in
                if lstFunctions{Position} = null then _ else lstFunctions{Position}(_))
    in
        Record.FromList(#"Enhance Tables",lstRecordFieldNames),

/* 
=========================================================================================
Extract Data
=========================================================================================
*/

// Creates a record of tables with extracted data from a nested table column
extractTables = (recs as record) =>
    let
        lstRecordFieldNames = Record.FieldNames(recs),
        lstTables = Record.ToList(recs),

        #"Combine Tables" = List.Transform(lstTables, each Table.Combine(_[Content]))
    in
        Record.FromList(#"Combine Tables",lstRecordFieldNames),

/* 
=========================================================================================
Cleaning Data
=========================================================================================
*/

// Cleans text in a table for all columns
cleanText = (tbl as table) =>
    let
        lstColumnNames = Table.ColumnNames(tbl),

        lstFunctions_Trim = List.Transform(lstColumnNames, (TextValue) => each Text.Trim(Text.From(_))),
        nlstTransformations_Trim = List.Zip({lstColumnNames,lstFunctions_Trim}),

        lstReplaceValues = {"","N","0", 0, "NULL","#(cr)"},

        replaceValues = (tbl as table, lstColumnNames as list, lstReplaceValues as list, optional x) =>
            let
                CurrentLoopPosition = if x = null then 0 else x,
                NextLoopPosition = CurrentLoopPosition + 1,
                EndLoopPosition = List.Count(lstReplaceValues),

                #"Replace Values" = 
                    // if the value to replace is a return carriage
                    if lstReplaceValues{CurrentLoopPosition} = "#(cr)" 

                    then Table.ReplaceValue(tbl, each lstReplaceValues{CurrentLoopPosition}, each " ", Replacer.ReplaceText,lstColumnNames)

                    // if the value to replace is not a return carriage
                    else Table.ReplaceValue(tbl, each lstReplaceValues{CurrentLoopPosition}, each null, Replacer.ReplaceValue,lstColumnNames)
            in 
                if NextLoopPosition < EndLoopPosition then @replaceValues(#"Replace Values", lstColumnNames, lstReplaceValues, NextLoopPosition) else #"Replace Values",       
            
        #"Trim Text" = Table.TransformColumns(tbl,nlstTransformations_Trim),
        #"Transform Table Values" =  replaceValues(#"Trim Text", lstColumnNames, lstReplaceValues)

    in
        #"Transform Table Values",
        

// Creates a record of tables that have been cleaned using a list of cleaning functions. Helpful for a preliminary general cleaning of data before a deep cleanse. lstFunctions takes a nested list of functions {function, optional parameters}
cleanRecords = (rec as record, lstFunctions as list) =>
    let
        lstRecordFieldNames = Record.FieldNames(rec),
        lstTables = Record.ToList(rec),

        cleanTable = (tbl as table, lstFunctions as list, RecordPosition as number, optional x) =>
            let
                CurrentLoopPosition = if x = null then 0 else x,
                NextLoopPosition = CurrentLoopPosition + 1,
                EndLoopPosition = List.Count(lstFunctions),

                Function = lstFunctions{CurrentLoopPosition}{0},
                Parameter = try lstFunctions{CurrentLoopPosition}{1} otherwise null,

                #"Clean" = if Parameter = null then Function(tbl) else Function(tbl, Parameter{RecordPosition})

            in 

                if NextLoopPosition < EndLoopPosition then @cleanTable(#"Clean", lstFunctions, RecordPosition, NextLoopPosition) else #"Clean",


        #"Clean Tables" = List.Transform(lstTables, each 
            let
                Position = List.PositionOf(lstTables,_)
            in
                cleanTable(_,lstFunctions, Position))

    in
        Record.FromList(#"Clean Tables", lstRecordFieldNames),


/* 
=========================================================================================
Filtering
=========================================================================================
*/

// Filters out columns from tables
filterColumns = (tbl as table, nlstColumnFilters as list) => 
    let
        lstColumnNames = Table.ColumnNames(tbl),
        lstColumns_Keep = filterList(lstColumnNames,nlstColumnFilters)
    in 
        Table.SelectColumns(tbl,lstColumns_Keep),


// Filters a list based on a nested list of {{keep},{remove}}
filterList = (BaseList as list, Selections as list) =>
    let
        lstKeep = Selections{0},
        lstRemove = Selections{1}
    in
        // If both lists are empty
        if List.IsEmpty(lstKeep) and List.IsEmpty(lstRemove) then BaseList

        // If remove list is empty
        else if not(List.IsEmpty(lstKeep)) and List.IsEmpty(lstRemove) then
            List.RemoveNulls(List.Transform(BaseList, each if List.AnyTrue(List.Transform(lstKeep, (value) => Text.Contains(_,value,Comparer.OrdinalIgnoreCase))) then _ else null))

        // If keep list is empty
        else if List.IsEmpty(lstKeep) and not(List.IsEmpty(lstRemove)) then
            let
                lstItems_Remove = List.RemoveNulls(List.Transform(BaseList, each if List.AnyTrue(List.Transform(lstRemove, (value) => Text.Contains(_,value,Comparer.OrdinalIgnoreCase))) then _ else null))
            in
                List.RemoveItems(BaseList, lstItems_Remove)
        
        // If both lists are not empty
        else 
            let 
                lstItems_Keep = List.RemoveNulls(List.Transform(BaseList, each if List.AnyTrue(List.Transform(lstKeep, (value) => Text.Contains(_,value,Comparer.OrdinalIgnoreCase))) then _ else null)),
                
                lstItems_Remove  = List.RemoveNulls(List.Transform(BaseList, each if List.AnyTrue(List.Transform(lstRemove, (value) => Text.Contains(_,value,Comparer.OrdinalIgnoreCase))) then _ else null))
            in
                List.RemoveItems(lstItems_Keep, lstItems_Remove),

/* 
=========================================================================================
Reordering
=========================================================================================
*/

// Reorder columns of a table based on a list of numbers (integer or decimal allowed; decimal is helpful if you want to put columns into groupings)
reorderColumns = (tbl as table, OrderList as list) =>
    let
        lstColumnNames = Table.ColumnNames(tbl),
        
        nlstNewColumnPositions = List.Generate(()=>0,each _ < List.Count(lstColumnNames), each _+1, each 
        if OrderList{_} = null
        then {_,List.Count(lstColumnNames)+1+_/100,lstColumnNames{_}}
        else {_,OrderList{_},lstColumnNames{_}}),

        lstColumnPositions_Sorted = List.Sort(nlstNewColumnPositions, each _{1}),
        lstNewColumnOrder = List.Transform(lstColumnPositions_Sorted, each List.Last(_))
        
    in

        Table.ReorderColumns(tbl,lstNewColumnOrder),


// Reorder values of a list based on a list of numbers (integer or decimal allowed; decimal is helpful if you want to put columns into groupings)
reorderList = (lst as list, OrderList as list) =>
    let
        lstColumnNames = lst,
        
        nlstNewColumnPositions = List.Generate(()=>0,each _ < List.Count(lstColumnNames), each _+1, each 
        if OrderList{_} = null
        then {_,List.Count(lstColumnNames)+1+_/100,lstColumnNames{_}}
        else {_,OrderList{_},lstColumnNames{_}}),

        lstColumnPositions_Sorted = List.Sort(nlstNewColumnPositions, each _{1}),
        lstNewColumnOrder = List.Transform(lstColumnPositions_Sorted, each List.Last(_))
        
    in

        lstNewColumnOrder,

/* 
=========================================================================================
Data Types
=========================================================================================
*/

// Update data types of a table
updateDataTypes = (tbl as table) =>
    let
        createList_DataTypes = (lstColumnNames as list) =>
        let
            lstPosition_DataTypes = List.Generate(() => 0, each _ < List.Count(lstColumnNames), each _ + 1, each List.PositionOf(List.Transform(glstDataTypes, (nListItem) => List.AnyTrue(List.Transform(nListItem{0}, (ListItem) => Text.Contains(lstColumnNames{_},ListItem,Comparer.OrdinalIgnoreCase)))),true)),

            Position_DefaultDataType = List.Count(glstDataTypes) - 1
        in
            List.Transform(lstPosition_DataTypes, each try glstDataTypes{_}{1} otherwise glstDataTypes{Position_DefaultDataType}{1}),

        lstColumnNames = Table.ColumnNames(tbl),
        lstDataTypes = createList_DataTypes(lstColumnNames),
        nlstTransformations = List.Zip({lstColumnNames,lstDataTypes})
    in
        Table.TransformColumnTypes(tbl,nlstTransformations),

/* 
=========================================================================================
Misc
=========================================================================================
*/

generateStep = (BaseList as list, fx as function) => List.Generate(()=> 0, each _ < List.Count(BaseList), each _ + 1, fx),


/* 
=========================================================================================
Table Statistics
=========================================================================================
*/

createStats = (rec as record) =>
    let
        lstRecordFieldNames = Record.FieldNames(rec),
        lstTables = Record.ToList(rec),

        lstRecordFieldNames_ColumnNames = generateStep(lstRecordFieldNames, each Text.Combine({lstRecordFieldNames{_}," Column Names"})),

        lstRecordFieldNames_ColumnPositions = generateStep(lstRecordFieldNames, each Text.Combine({lstRecordFieldNames{_}," Column Positions"})),

        lstStats_ColumnNames = generateStep(lstRecordFieldNames, each Table.ColumnNames(lstTables{_})),

        lstStats_ColumnPositions = generateStep(lstRecordFieldNames, each Table.FromList(List.Transform(lstStats_ColumnNames{_}, (Value) => Text.Combine({Text.From(List.PositionOf(lstStats_ColumnNames{_}),Value),Value},",")),null,{"Position","Column Name"})),

        lstRecordStats_Zip = List.Zip({lstStats_ColumnNames, lstStats_ColumnPositions}),
        lstRecordFieldNames_Zip = List.Zip({lstRecordFieldNames_ColumnNames, lstRecordFieldNames_ColumnPositions})


    in 
        Record.FromList(generateStep(lstRecordFieldNames, each Record.FromList(lstRecordStats_Zip{_},lstRecordFieldNames_Zip{_})),lstRecordFieldNames)

]

in 
    gblFunctions