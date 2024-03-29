module ColumnSetManagers
using DataStructures: OrderedRobinDict, Stack
using ..NameLists: NameID, NameList, top_level_id, unnamed_id, unnamed, max_id
import ..NestedIterators: RawNestedIterator, NestedIterator, cycle, repeat_each

import ..get_name
import ..get_id
import ..collect_tuple

#### ColumnSet ####
###################
"""A dict-like set of columns. The keys are Int64 ids for actual names that are stored
in the ColumnSetManager"""
mutable struct ColumnSet
    cols::Vector{Pair{NameID, RawNestedIterator}}
    len::Int64
end
ColumnSet() = ColumnSet(Pair{NameID, RawNestedIterator}[], 0)
function ColumnSet(p::Pair...)
    cs = ColumnSet(Pair{NameID, RawNestedIterator}[p...], 0)
    sort_keys!(cs)
    reset_length!(cs)
    return cs
end

# Dict Interface
function Base.empty!(cs::ColumnSet)
    empty!(cs.cols)
    cs.len = 0
end

Base.haskey(cs::ColumnSet, k) = insorted((k,0), cs.cols; by=first)

function Base.setindex!(cs::ColumnSet, v, k)
    insert!(cs, (k=>v))
    cs.len = max(cs.len, length(v))
    return cs
end

function Base.getindex(cs::ColumnSet, k)
    i = searchsortedfirst(cs.cols, (k,0); by=first)
    p = cs.cols[i]
    if p[1] != k
        throw(KeyError(k))
    end
    return p[2]
end


"""
    get_column!(cols::ColumnSet, name, default::RawNestedIterator)
Get a column from a set with a given name, if no column with that name is found
construct a new column with same length as column set
"""
function Base.pop!(cols::ColumnSet, name_id::NameID, default::RawNestedIterator)
    return if haskey(cols,name_id)
        last(pop!(cols,name_id))
    else
        cycle(default, column_length(cols))
    end
end
function Base.pop!(cs::ColumnSet, k)
    i = searchsortedfirst(cs.cols, (k,0); by=first)
    p = popat!(cs.cols, i)
    if p[1] != k
        throw(KeyError(k))
    end
    return p
end

function Base.push!(cs::ColumnSet, p::Pair) 
    push!(cs.cols, p)
end

function Base.merge!(cs1::ColumnSet, cs2::ColumnSet)
    append!(cs1.cols, cs2.cols)
    sort_keys!(cs1)
    cs1.len = max(cs1.len, cs2.len)
    return cs1
end

Base.keys(cs::ColumnSet) = (first(p) for p in cs.cols)
Base.values(cs::ColumnSet) = (last(p) for p in cs.cols)
Base.pairs(cs::ColumnSet) = (p for p in cs.cols)
Base.isequal(::ColumnSet, ::ColumnSet) = throw(ErrorException("To compare ColumnSet with ColumnSet, you must pass a ColumnSetManager."))
function Base.isequal(cs::ColumnSet, o::ColumnSet, csm)
    all(isequal.(keys(cs), keys(o))) && all(isequal.(values(cs), values(o), Ref(csm)))
end

Base.length(cs::ColumnSet) = length(cs.cols)
Base.insert!(cs::ColumnSet, p::Pair) = insert!(cs.cols, searchsortedfirst(cs.cols, p; by=first), p)

sort_keys!(cs::ColumnSet) = sort!(cs.cols; by=first, alg=InsertionSort)
"""Get the length of the longest column. This is almost always the length of all columns in the set
except in the midst of merging multiple sets together"""
column_length(cols::ColumnSet) = cols.len

"""Check for the longest column in a set and update column length of set"""
function reset_length!(cs::ColumnSet)
    len = 0
    for v in values(cs)
        len = max(len, length(v))
    end
    set_length!(cs, len)
    return cs
end

"""Force column length of set"""
function set_length!(cs::ColumnSet, l)
    cs.len = l
end


"""
The ColumnSetManager creates IDs and stores for keys in the input data and for full field paths.
It also keeps ColumnSets that are no longer in use and recycles them when a new ColumnSet is needed
"""
struct ColumnSetManager{T}
    # todo using dict here reduced JET errors, need to test performance later
    name_to_id::OrderedRobinDict{Any, NameID}
    id_generator::T
    column_sets::Stack{ColumnSet}
    name_list_collector::Vector{NameID}
end
function ColumnSetManager()
    name_to_id = OrderedRobinDict{Any, NameID}(unnamed => unnamed_id)
    id_generator = Iterators.Stateful(Iterators.countfrom(2))
    column_sets = Stack{ColumnSet}()
    name_list_collector = NameID[]
    return ColumnSetManager(
        name_to_id, 
        id_generator, 
        column_sets, 
        name_list_collector, 
    )
end


"""
    get_id(csm::ColumnSetManager, name)
Get an id for a new or existing name within a field path
"""
function get_id(csm::ColumnSetManager, name)
    if haskey(csm.name_to_id, name)
        return csm.name_to_id[name]
    end
    id = NameID(first(csm.id_generator))
    csm.name_to_id[name] = id
    return id
end

get_id(::ColumnSetManager, name::NameID) = name
    

"""
    get_id(csm::ColumnSetManager, field_path::Cons{Int64})

Get an id for the linked list of ids that constitute a field path in the core loop
"""
function get_id(csm::ColumnSetManager, name_list::NameList)
    field_path = collect_name_ids(csm::ColumnSetManager, name_list::NameList)
    path_tuple = collect_tuple(field_path)
    return get_id(csm, path_tuple)
end

"""Return a vector of NameIDs based on the given NameList. CAREFUL the returned vector is a buffer which is overwritten the
next time `collect_name_ids` is called."""
function collect_name_ids(csm::ColumnSetManager, name_list::NameList)
    empty!(csm.name_list_collector)
    head::NameList = name_list
    while head.i != top_level_id
        push!(csm.name_list_collector, head.i)
        head = head.tail_i
    end
    # need to reverse field path because we stack the last on top as we descend through the data structure
    return reverse!(csm.name_list_collector)
end


"""
    get_id_for_path(csm::ColumnSetManager, field_path:Tuple)

Given a path of actual names, create an id for each name, then create a id for the
new id path, return that final id
"""
function get_id_for_path(csm::ColumnSetManager, field_path::Tuple)
    path_tuple = tuple((get_id(csm, name) for name in field_path)...)
    id = get_id(csm, path_tuple)
    return id
end

"""
    get_name(csm::ColumnSetManager, id)
Return the name associated with an id
"""
function get_name(csm::ColumnSetManager, id::NameID)
    return csm.name_to_id.keys[id.id]
end

"""
    reconstruct_field_path(csm::ColumnSetManager, id)
Given an id for a field_path, reconstruct a tuple of actual names.
For heterogenous nodes (with both containers and values) the field path has an "unnamed" value. This
is dropped when reconstructing the field path.
"""
function reconstruct_field_path(csm::ColumnSetManager, id::NameID, include_unnamed=false)
    id_path = get_name(csm, id)
    symbol_gen = include_unnamed ?
        (Symbol(get_name(csm, name_id)) for name_id in id_path) :
        (Symbol(get_name(csm, name_id)) for name_id in id_path if name_id != unnamed_id)
    return tuple(symbol_gen...)
end

"""
    get_column_set(csm::ColumnSetManager)
Get a new ColumnSet from the manager
"""
function get_column_set(csm::ColumnSetManager)
    col_set = if !isempty(csm.column_sets)
        pop!(csm.column_sets)
    else
        ColumnSet()
    end
    return col_set
end

"""
    free_column_set!(csm::ColumnSetManager, column_set::ColumnSet)
Return a ColumnSet so that it can be recycled in future `get_column_set` calls
"""
function free_column_set!(csm::ColumnSetManager, column_set::ColumnSet)
    empty!(column_set)
    push!(csm.column_sets, column_set)
end

"""
    Base.merge!(csm::ColumnSetManager, cs1, cs2)    
Merge cs2 into cs1 and free cs2
"""
function Base.merge!(csm::ColumnSetManager, cs1::ColumnSet, cs2::ColumnSet)
    merge!(cs1, cs2)
    free_column_set!(csm, cs2)
    return cs1
end

"""
    init_column_set(csm::ColumnSetManager, name::Cons{Int64}, data)
Create a new ColumnSet containing an id for name and a RawNestedIterator around data
"""
function init_column_set(csm::ColumnSetManager, name::NameList, data)
    col = RawNestedIterator(csm, data)
    cs = get_column_set(csm)
    id = get_id(csm, name)
    cs[id] = col
    return cs
end

"""
    build_final_column_set(csm::ColumnSetManager, raw_cs)
Take a ColumnSet with ID keys and reconstruct a column_set with actual names keys
"""
function build_final_column_set(csm::ColumnSetManager, raw_cs::ColumnSet)
    final_cs = OrderedRobinDict{Tuple, NestedIterator}()
    for (raw_id, column) in pairs(raw_cs)
        field_path = reconstruct_field_path(csm, raw_id)
        final_cs[field_path] = NestedIterator(csm, column)
    end
    return final_cs
end

"""
    get_total_length(vec_of_col_sets)
Add up the column_length of all columns in a vector
"""
function get_total_length(vec_of_col_sets)
    len = 0
    for col_set in vec_of_col_sets
        len += column_length(col_set)
    end
    return len
end


"""apply_in_place!(cols, f, args...)
Apply f to each key, column pair by popping the value and readding
the key (this prevents mismatching key hashes after manipulating a ColumnSet)"""
function apply_in_place!(cols, f, args...)
    for i in eachindex(cols)
        k, v = cols[i]
        val = f(v, args...)
        cols[i] = Pair{NameID, RawNestedIterator}(k,val)
    end
end

"""
repeat_each_column!(cols, n)

Given a column set, apply repeat_each to all columns in place
"""
function repeat_each_column!(col_set::ColumnSet, n)
    apply_in_place!(col_set.cols, repeat_each, n)
    col_set.len *= n
    return col_set
end

"""
cycle_columns_to_length!(cols::ColumnSet) 

Given a column set where the length of all columns is some factor of the length of the longest
column, cycle all the short columns to match the length of the longest
"""
function cycle_columns_to_length!(col_set::ColumnSet)
    longest = col_set.len
    apply_in_place!(col_set.cols, cols_to_length, longest)
    return col_set
end
function cols_to_length(val, longest)
    catchup_mult = longest ÷ length(val)
    return cycle(val, catchup_mult)
end

"""
    get_first_key(cs::ColumnSet)
Return the lowest value id key from a columnset
"""
get_first_key(cs::ColumnSet) = length(cs) > 0 ? first(first(cs.cols)) : max_id

end # ColumnSetManagers
