/* 
=========================================================================================
Staging - ETL
=========================================================================================
# Extract Tables
Extracts and combines all files from the `100 Data` Folder into their respective tables
1. NBI
2. DOT (NC One Map)
3. Counties
4. Assignments

# Transform Tables
Performs two types of cleaning: general and deep.

## General Cleaning
A broad cleaning of all tables from the extracted data
    1. Partition Data into Years
    2. Transform Blanks to Nulls
    3. Trim all data fields
    4. Update Data Types

## Deep Cleaning
A more focused cleaning tailored to the nuances of each table

# Output Tables
    1. DOT Table
    2. NBI Table
    3. Assigned Structures Table
    4. Assignments Table
    5. Updated Assigned Structures Table
=========================================================================================
*/

let
// Global Defintions
gblObjects = #shared[gblObjects],
    csv_RawNBI = gblObjects[csv_RawNBI],
    csv_RawDOT = gblObjects[csv_RawDOT],
    csv_RawCounties = gblObjects[csv_RawCounties],
    csv_RawAssignments = gblObjects[csv_RawAssignments],

    csv_dtqAssignedStructures = gblObjects[csv_dtqAssignedStructures],

    csv_atuAssignedStructures = gblObjects[csv_atuAssignedStructures],

gblFunctions = gblObjects[gblFunctions],
    importFiles = gblFunctions[importFiles],
    enhanceTables = gblFunctions[enhanceTables],
    extractTables = gblFunctions[extractTables],
    cleanText = gblFunctions[cleanText],
    cleanRecords = gblFunctions[cleanRecords],
    filterColumns = gblFunctions[filterColumns],
    filterList = gblFunctions[filterList],
    generateStep = gblFunctions[generateStep],
    reorderColumns = gblFunctions[reorderColumns],
    updateDataTypes = gblFunctions[updateDataTypes],


/* 
=========================================================================================
Extract Data Tables
=========================================================================================
*/

RawFiles =
[
    NBI = csv_RawNBI,
    DOT = csv_RawDOT,
    Counties = csv_RawCounties,
    Assignments = csv_RawAssignments,

    dtqAssignedStructures = csv_dtqAssignedStructures,

    atuAssignedStructures = csv_atuAssignedStructures
],

ExtractedTables = extractTables(importFiles(RawFiles)),

/* 
=========================================================================================
Transform Data Tables - General Cleaning
=========================================================================================
*/

lstNBIFilters = {{"008","027","106", "058", "059", "060", "062", "101", "049", "052","003"},{}},
lstDOTFilters = {{"brdg","intrsc","carried","county","division","_dd","google","superstruc","substruc"},{"bsip"}},

nlstColumnFilters = List.Transform(Record.FieldNames(RawFiles), each if _ = "NBI" then lstNBIFilters else if _ = "DOT" then lstDOTFilters else {{},{}}),

lstCleaningFunctions = 
{
    {filterColumns, nlstColumnFilters},
    {cleanText}
},

GeneralCleaning = cleanRecords(ExtractedTables, lstCleaningFunctions),

/* 
=========================================================================================
Deep Cleaning [DOT]
=========================================================================================
*/

DOT_GeneralCleaning = GeneralCleaning[DOT],
DOT_lstColumnNames = Table.ColumnNames(DOT_GeneralCleaning),

DOT =
[
    #"Rename Columns" =
        let

            nlstColumnNames_Split = List.Transform(DOT_lstColumnNames, each Text.Split(Text.Proper(Text.Replace(_,"_"," "))," ")),

            nlstColumnNames_Updated = List.Transform(nlstColumnNames_Split, each List.RemoveNulls(List.Transform(_, (ListItem) => 
                if List.AnyTrue(List.Transform({"dd","nm"}, (TextValue) => Text.Contains(ListItem,TextValue, Comparer.OrdinalIgnoreCase))) then null

                else if Text.Contains(ListItem,"brdg", Comparer.OrdinalIgnoreCase) then "Bridge"

                else if Text.Contains(ListItem,"nbr", Comparer.OrdinalIgnoreCase) then "Number"


                else if List.AnyTrue(List.Transform({"f","ftr"}, (TextValue) => Text.Contains(ListItem,TextValue, Comparer.OrdinalIgnoreCase))) then "Feature"

                else if Text.Contains(ListItem,"typ", Comparer.OrdinalIgnoreCase) then "Type"

                else if Text.Contains(ListItem,"intrsc", Comparer.OrdinalIgnoreCase) then "Intersected"

                else if Text.Contains(ListItem,"long", Comparer.OrdinalIgnoreCase) then "Longitude"

                else if Text.Contains(ListItem,"lat", Comparer.OrdinalIgnoreCase) then "Latitude"

                else if Text.Contains(ListItem,"google", Comparer.OrdinalIgnoreCase) then "Google Maps"

                else if Text.Contains(ListItem,"link", Comparer.OrdinalIgnoreCase) then "URL"

                else if Text.Contains(ListItem,"superst", Comparer.OrdinalIgnoreCase) then "Substructure"

                else if Text.Contains(ListItem,"substr", Comparer.OrdinalIgnoreCase) then "Superstructure"
                else ListItem))),

            lstNewColumnNames = List.Transform(nlstColumnNames_Updated, each Text.Combine(_," ")),
            nlstTransformations_Rename = List.Zip({DOT_lstColumnNames, lstNewColumnNames})

        in

            Table.RenameColumns(DOT_GeneralCleaning, nlstTransformations_Rename),

    #"Reorder Columns" = reorderColumns(#"Rename Columns",{1.0,1.1,null,null,1.2,1.3,2.1,2.0,2.2,3.1,3.0}),

    #"Final Output" = #"Reorder Columns"
],

/* 
=========================================================================================
Deep Cleaning [NBI]
=========================================================================================
*/

NBI_GeneralCleaning = GeneralCleaning[NBI],
NBI_lstColumnNames = Table.ColumnNames(NBI_GeneralCleaning),

Counties_ExtractedTable = ExtractedTables[Counties],

NBI =
[
#"Transform Structure Numbers" =
    let
        txtColumnName_StructureNumber = List.First(filterList(NBI_lstColumnNames,{{"structure_number"},{}})),

        txtKeyColumnName_NBI = List.First(filterList(NBI_lstColumnNames,{{"county_code"},{}})),

        // Counties
        Counties_lstColumnNames = Table.ColumnNames(Counties_ExtractedTable),
        Counties_txtKeyColumnName = List.First(filterList(Counties_lstColumnNames,{{"federal"},{}})),
        lstColumnName_StateCountyNumber = filterList(Counties_lstColumnNames,{{"state"},{}}),

        txt_StructureNumberLast3 = "Structure Number (Last 3)",
        txt_StateCountyCode = "State County Code",
        txt_NewStructureNumber = "New Structure Number",

        lstExistingHelperColumnNames = filterList(Table.ColumnNames(NBI_GeneralCleaning),{{"003"},{}}),
        lstHelperColumnNames = List.Union({{txt_StructureNumberLast3, txt_StateCountyCode, txt_NewStructureNumber},lstExistingHelperColumnNames}),

        // Select structures without any letters in the identifier
        #"Remove Non Assignable Structures" = Table.SelectRows(NBI_GeneralCleaning, each Text.Length(Text.Select(Text.Lower(Record.Field(_,txtColumnName_StructureNumber)),{"a".."z"})) = 0),
        
        #"Find Last Digits of Structure Number" = Table.AddColumn(#"Remove Non Assignable Structures",txt_StructureNumberLast3, each Text.End(Record.Field(_,txtColumnName_StructureNumber),3)), 

        #"Add State County Code" =  Table.ExpandTableColumn(Table.NestedJoin(#"Find Last Digits of Structure Number",txtKeyColumnName_NBI, Counties_ExtractedTable, Counties_txtKeyColumnName, "Counties",JoinKind.LeftOuter),"Counties",lstColumnName_StateCountyNumber),

        #"Add New Structure Number Column" = Table.AddColumn(#"Add State County Code",txt_NewStructureNumber, each Text.Combine({Record.Field(_,txt_StateCountyCode),Record.Field(_,txt_StructureNumberLast3)})),

        #"Update Stale Structure Number Column" = Table.ReplaceValue(#"Add New Structure Number Column", each Record.Field(_,txtColumnName_StructureNumber), each Record.Field(_,txt_NewStructureNumber),Replacer.ReplaceValue, {txtColumnName_StructureNumber}),

        #"Remove Helper Columns" = Table.RemoveColumns(#"Update Stale Structure Number Column",lstHelperColumnNames)

    in

        #"Remove Helper Columns",

uNBI_lstColumnNames = Table.ColumnNames(#"Transform Structure Numbers"),

#"Rename Columns" =
    let
        nlstColumnNames_Split = List.Transform(uNBI_lstColumnNames, each Text.Split(Text.Proper(Text.BeforeDelimiter(Text.Replace(_,"_"," ")," ",{0,RelativePosition.FromEnd}))," ")),

        nlstColumnNames_Updated = List.Transform(nlstColumnNames_Split, each List.RemoveNulls(List.Transform(_, (Textvalue) => 
            if Text.Contains(Textvalue,"mt", Comparer.OrdinalIgnoreCase) then null

            else if Text.Contains(Textvalue,"len", Comparer.OrdinalIgnoreCase) then "Length"

            else if Text.Contains(Textvalue,"cond", Comparer.OrdinalIgnoreCase) then "Condition"

            else if Text.Contains(Textvalue,"inspect", Comparer.OrdinalIgnoreCase) then "Inspection"

            else if Text.Contains(Textvalue,"freq", Comparer.OrdinalIgnoreCase) then "Frequency"

            else if Text.Contains(Textvalue,"months", Comparer.OrdinalIgnoreCase) then "(Months)"

            else if Text.Contains(Textvalue,"temp", Comparer.OrdinalIgnoreCase) then "Temporary"

            else Textvalue))),

        lstNewColumnNames = List.Transform(nlstColumnNames_Updated, each Text.Combine(_," ")),
        nlstTransformations_Rename = List.Zip({uNBI_lstColumnNames, lstNewColumnNames})
    in
        Table.RenameColumns(#"Transform Structure Numbers",nlstTransformations_Rename),
    
#"Reorder Columns" = reorderColumns(#"Rename Columns",{0,3.0,1.0,1.2,2.0,2.1,2.2,2.3,0.1,3.1}),

#"Final Output" = #"Reorder Columns"

]


in

[
    // Raw Tables - Unclean
    RawTables = ExtractedTables,
    GeneralCleaning = GeneralCleaning,

    // Raw Tables - Cleaned
    DOT = DOT[Final Output],
    NBI = NBI[Final Output],
    Assignments = GeneralCleaning[Assignments],

    // Queried Data Tables
    dtqAssignedStructures = Table.Sort(GeneralCleaning[dtqAssignedStructures],
    {
        {"Structure Number", Order.Ascending}
    }),

    // Updated Analysis Tables
    atuAssignedStructures = GeneralCleaning[atuAssignedStructures]

]


