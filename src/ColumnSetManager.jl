"""A null "name" for the top level input"""
top_level() = NameList'.Empty(Nil{Int64}())
"""the id for unnamed key. This happens when an array has loose values and containers"""
unnamed_id() = 1
"""the name to use for unnamed keys"""
unnamed() = :expand_nested_data_unnamed

"""
The ColumnSetManager creates IDs and stores for keys in the input data and for full field paths.
It also keeps ColumnSets that are no longer in use and recycles them when a new ColumnSet is needed
"""
struct ColumnSetManager{T}
    name_to_id::OrderedRobinDict{Any, Int64}
    id_generator::T
    column_sets::Stack{ColumnSet}
end

function ColumnSetManager()
    name_to_id = OrderedRobinDict{Any, Int64}(unnamed() => unnamed_id())
    id_generator = Iterators.Stateful(Iterators.countfrom(2))
    column_sets = Stack{ColumnSet}()
    return ColumnSetManager(name_to_id, id_generator, column_sets)
end

"""
    get_id(csm::ColumnSetManager, name)
Get an id for a new or existing name within a field path
"""
function get_id(csm::ColumnSetManager, name)
    if haskey(csm.name_to_id, name)
        return csm.name_to_id[name]
    end
    id = first(csm.id_generator)
    csm.name_to_id[name] = id
    return id
end

"""
    get_id(csm::ColumnSetManager, field_path::Cons{Int64})

Get an id for the linked list of ids that constitute a field path in the core loop
"""
function get_id(csm::ColumnSetManager, name_list::NameList)
    field_path = @cases name_list begin
        Head(f) => f 
        Empty => throw(ErrorException("Cannot create an ID for an empty list"))
    end
    # need to reverse field path because we stack the last on top as we descend through the data structure
    path_tuple = tuple((i for i in reverse(field_path))...)
    return get_id(csm, path_tuple)
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
function get_name(csm::ColumnSetManager, id)
    return csm.name_to_id.keys[id]
end

"""
    reconstruct_field_path(csm::ColumnSetManager, id)
Given an id for a field_path, reconstruct a tuple of actual names
"""
function reconstruct_field_path(csm::ColumnSetManager, id)
    id_path = get_name(csm, id)
    return tuple((Symbol(get_name(csm, name_id)) for name_id in id_path)...)
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
function Base.merge!(csm::ColumnSetManager, cs1, cs2)
    merge!(cs1, cs2)
    free_column_set!(csm, cs2)
    return cs1
end

"""
    init_column_set(csm::ColumnSetManager, name::Cons{Int64}, data)
Create a new ColumnSet containing an id for name and a NestedIterator around data
"""
function init_column_set(csm::ColumnSetManager, name::NameList, data)
    col = NestedIterator(data)
    cs = get_column_set(csm)
    id = get_id(csm, name)
    cs[id] = col
    return cs
end

"""
    build_final_column_set(csm::ColumnSetManager, raw_cs)
Take a ColumnSet with ID keys and reconstruct a column_set with actual names keys
"""
function build_final_column_set(csm::ColumnSetManager, raw_cs)
    # todo -- we could track the longest field_path and then make the tuple length known
    # todo -- can the final columnset be changed to symbols at this point?
    final_cs = OrderedRobinDict{Tuple, NestedIterator}()
    for (raw_id, column) in pairs(raw_cs)
        field_path = reconstruct_field_path(csm, raw_id)
        final_cs[field_path] = column
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
        cols[i] = Pair(k,val)
    end
end

"""
repeat_each_column!(cols, n)

Given a column set, apply repeat_each to all columns in place
"""
function repeat_each_column!(col_set::ColumnSet, n)
    apply_in_place!(col_set.cols, repeat_each, n)
    col_set.len *= n
end

"""
cycle_columns_to_length!(cols::ColumnSet) 

Given a column set where the length of all columns is some factor of the length of the longest
column, cycle all the short columns to match the length of the longest
"""
function cycle_columns_to_length!(cols::ColumnSet)
    longest = cols.len
    apply_in_place!(cols.cols, cols_to_length, longest)
    return cols
end
function cols_to_length(val, longest)
    catchup_mult = longest ÷ length(val)
    return cycle(val, catchup_mult)
end

"""
    get_first_key(cs::ColumnSet)
Return the lowest value id key from a columnset
"""
get_first_key(cs::ColumnSet) = length(cs) > 0 ? first(first(cs.cols)) : typemax(Int64)

"""
    get_column!(cols::ColumnSet, name, default::NestedIterator)
Get a column from a set with a given name, if no column with that name is found
construct a new column with same length as column set
"""
function pop_column!(cols::ColumnSet, name, default::NestedIterator)
    return if haskey(cols,name)
        last(pop!(cols,name))
    else
        cycle(default, column_length(cols) ÷ length(default))
    end
end


"""Return a missing column for each member of a child path"""
function make_missing_column_set(csm, path_node::Node)
    missing_column_set = get_column_set(csm)

    for value_node in get_all_value_nodes(path_node)
        field_path = get_field_path(value_node)
        id = get_id_for_path(csm, field_path)
        missing_column_set[id] = get_default(value_node)
    end

    return missing_column_set
end


