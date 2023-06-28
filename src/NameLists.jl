module NameLists
#### Linked List for Key/Names ####
###################################
# An ID refering to a key/name in the input
struct NameID
    id::Int64
end
Base.isless(n::NameID, o::NameID) = n.id < o.id
# A link in a list of name IDs
struct NameLink
    tail_i::Int64
    name_id::NameID
end
# Points to current head of a NameList
struct NameList
    i::Int64
end
function NameList(csm, name_list::NameList, new_id::NameID)
    name_list_links = csm.name_list_links
    prev_i = name_list.i
    push!(name_list_links, NameLink(prev_i, new_id))
    return NameList(first(csm.link_i_generator))
end


#### Constants ####
###################

const no_name_id = NameID(-1)
"""A null NameList for the top level input"""
const top_level = NameList(0)
"""A NameID for TOP_LEVEL"""
const top_level_id = NameID(0)
"""the id for unnamed key. This happens when an array has loose values and containers"""
const unnamed_id = NameID(1)
"""the name to use for unnamed keys"""
const unnamed = :expand_nested_data_unnamed
const max_id = NameID(typemax(Int64))
end