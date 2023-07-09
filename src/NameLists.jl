module NameLists
#### Linked List for Key/Names ####
###################################
# An ID refering to a key/name in the input
"""Represents an ID for a name stored in a ColumnSetManager"""
struct NameID
    id::Int64
end
Base.isless(n::NameID, o::NameID) = n.id < o.id

# Points to current head of a NameList
"""NameList is a node in a linked list of NameIDs"""
struct NameList
    tail_i::Union{NameList, Nothing}
    i::NameID
end
NameList() = NameList(nothing, top_level_id)

#### Constants ####
###################
"""Represents a missing name ID"""
const no_name_id = NameID(-1)
"""A NameID for TOP_LEVEL"""
const top_level_id = NameID(0)
"""the id for unnamed key. This happens when an array has loose values and containers"""
const unnamed_id = NameID(1)
"""the name to use for unnamed keys"""
const unnamed = :expand_nested_data_unnamed
const max_id = NameID(typemax(Int64))
end