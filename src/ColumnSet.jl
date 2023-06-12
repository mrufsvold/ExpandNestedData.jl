ColumnPair = Pair{Int64, NestedIterator}
mutable struct ColumnSet
    cols::Vector{ColumnPair}
    len::Int64
end
ColumnSet() = ColumnSet(ColumnPair[], 0)
function ColumnSet(p::Pair...)
    cs = ColumnSet(ColumnPair[p...], 0)
    sort_keys!(cs)
    reset_length!(cs)
    return cs
end
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

function merge!(cs1::ColumnSet, cs2::ColumnSet)
    append!(cs1.cols, cs2.cols)
    sort_keys!(cs1)
    cs1.len = max(cs1.len, cs2.len)
    return cs1
end

Base.keys(cs::ColumnSet) = (first(p) for p in cs.cols)
Base.values(cs::ColumnSet) = (last(p) for p in cs.cols)
Base.length(cs::ColumnSet) = length(cs.cols)

sort_keys!(cs::ColumnSet) = sort!(cs.cols; by=first, alg=InsertionSort)
Base.insert!(cs::ColumnSet, p::Pair) = insert!(cs.cols, searchsortedfirst(cs.cols, p; by=first), p)

column_length(cols::ColumnSet) = cols.len
function reset_length!(cs::ColumnSet)
    len = 0
    for v in values(cs)
        len = max(len, length(v))
    end
    set_length!(cs, len)
    return cs
end

function set_length!(cs::ColumnSet, l)
    cs.len = l
end

const top_level = Nil{Int64}()
const unnamed = 1
const empty_column = NestedIterator()

struct ColumnSetManager
    name_to_id::OrderedRobinDict{Any, Int64}
    id_generator::Channel{Int64}
    column_sets::Stack{ColumnSet}
end

function ColumnSetManager()
    name_to_id = OrderedRobinDict{Any, Int64}(:unnamed => unnamed)
    id_generator = Channel{Int64}() do ch
        i = 2
        while true
            put!(ch, i)
            i +=1
        end
    end
    column_sets = Stack{ColumnSet}()
    return ColumnSetManager(name_to_id, id_generator, column_sets)
end

function get_id(csm::ColumnSetManager, name)
    if haskey(csm.name_to_id, name)
        return csm.name_to_id[name]
    end
    id = take!(csm.id_generator)
    csm.name_to_id[name] = id
    return id
end

"""
    get_id(csm::ColumnSetManager, field_path::Cons{Int64})

Get an id for the linked list of ids that constitute a field path in the core loop
"""
function get_id(csm::ColumnSetManager, field_path::Cons{Int64})
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

function get_name(csm::ColumnSetManager, id)
    return csm.name_to_id.keys[id]
end


function reconstruct_field_path(csm::ColumnSetManager, id)
    id_path = get_name(csm, id)
    return tuple((get_name(csm, name_id) for name_id in id_path)...)
end

function get_column_set(csm::ColumnSetManager)
    col_set = if !isempty(csm.column_sets)
        pop!(csm.column_sets)
    else
        ColumnSet()
    end
    return col_set
end

function free_column_set!(csm::ColumnSetManager, column_set::ColumnSet)
    empty!(column_set)
    push!(csm.column_sets, column_set)
end

function merge!(csm::ColumnSetManager, cs1, cs2)
    merge!(cs1, cs2)
    free_column_set!(csm, cs2)
    return cs1
end

function init_column_set(csm::ColumnSetManager, name::Cons{Int64}, data)
    col = NestedIterator(data)
    cs = get_column_set(csm)
    id = get_id(csm, name)
    cs[id] = col
    return cs
end

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



function get_unique_names(vec_of_col_sets)
    unique_names = IntSet()
    for col_set in vec_of_col_sets
        push!(unique_names, keys(col_set)...)
    end
    return unique_names
end

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
function make_missing_column_set(csm, path_node)
    missing_column_set = get_column_set(csm)

    for value_node in get_all_value_nodes(path_node)
        field_path = get_field_path(value_node)
        id = get_id_for_path(csm, field_path)
        missing_column_set[id] = get_default(value_node)
    end

    return missing_column_set
end


