/* 
=========================================================================================
Global - Lists
=========================================================================================
# Script Definition
This script contains lists that can be used throughout the source folder.
=========================================================================================
*/

let
// Nested List of {{phrases to find},{data type}}, the last list should be the default type
glstDataTypes = 
{
    {{"division","typmain","type_0","type_1","kind","cond","year","month"}, Int64.Type},
    {{"len","width","_dd","latitude","longitude"}, Decimal.Type},
    {{}, Text.Type}
},

// List of duplicates that are to be removed from dtAssignedStructures
glstDuplicates =
{
    {"170378", 1},
    {"870106", 1},
    {"910003", 1},
    {"910007", 1},
    {"910011", 1},
    {"910015", 1},
    {"910018", 1},
    {"910023", 1},
    {"910025", 1},
    {"910039", 1},
    {"910044", 1},
    {"910045", 1},
    {"910060", 1},
    {"910063", 1},
    {"910085", 1},
    {"910095", 1},
    {"910096", 1},
    {"910097", 1},
    {"910102", 1},
    {"910317", 1},
    {"910352", 1},
    {"910561", 1},
    {"910650", 1}
}

in

[
    glstDataTypes = glstDataTypes,
    glstDuplicates = glstDuplicates
]