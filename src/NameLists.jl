module NameLists
#### Linked List for Key/Names ####
###################################
# An ID refering to a key/name in the input
struct NameID
    id::Int64
end
Base.isless(n::NameID, o::NameID) = n.id < o.id

# Points to current head of a NameList
struct NameList
    tail_i::Union{NameList, Nothing}
    i::NameID
end
NameList() = NameList(nothing, top_level_id)

#### Constants ####
###################

const no_name_id = NameID(-1)
"""A NameID for TOP_LEVEL"""
const top_level_id = NameID(0)
"""the id for unnamed key. This happens when an array has loose values and containers"""
const unnamed_id = NameID(1)
"""the name to use for unnamed keys"""
const unnamed = :expand_nested_data_unnamed
const max_id = NameID(typemax(Int64))
end