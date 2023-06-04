##### NameValueContainer #####
##############################

"""NameValueContainer is an abstraction on Dict and DataType structs so that we can get their
contents without worrying about `getkey` or `getproperty`, etc.
"""
NameValueContainer = Union{StructTypes.DictType, StructTypes.DataType}
Container = Union{StructTypes.DictType, StructTypes.DataType, StructTypes.ArrayType}

is_NameValueContainer(t) = typeof(StructTypes.StructType(t)) <: NameValueContainer
is_container(t) = typeof(StructTypes.StructType(t)) <: Container
is_value_type(t::Type) = !is_container(t) && isconcretetype(t)

"""Check if any elements in an iterator are subtypes of NameValueContainer"""
function has_namevaluecontainer_element(itr)
    if eltype(itr) == Any
        return itr .|> eltype .|> is_NameValueContainer |> any
    else
        return itr |> eltype |> get_member_types .|> is_NameValueContainer |> any
    end
end
get_member_types(::Type{T}) where T = T isa Union ? Base.uniontypes(T) : [T]

"""Define a pairs iterator for all DataType structs"""
get_pairs(x::T) where T = get_pairs(StructTypes.StructType(T), x)
get_pairs(::StructTypes.DataType, x::T) where T = ((p, getproperty(x, p)) for p in fieldnames(T))
get_pairs(::StructTypes.DictType, x) = pairs(x)

"""Get the keys/names of any NameValueContainer"""
get_names(x::T) where T = get_names(StructTypes.StructType(T), x)
get_names(::StructTypes.DataType, x::T) where T = (n for n in fieldnames(T))
get_names(::StructTypes.DictType, x) = keys(x)


get_value(x::T, name) where T = get_value(StructTypes.StructType(T), x, name)
get_value(::StructTypes.DataType, x, name) = getproperty(x, name)
get_value(::StructTypes.DictType, x, name) = x[name]

get_value(x::T, name, default) where T = get_value(StructTypes.StructType(T), x, name, default)
get_value(::StructTypes.DataType, x, name, default) = hasproperty(x, name) ? getproperty(x, name) : default
get_value(::StructTypes.DictType, x, name, default) = get(x, name, default)

##### NestedIterator #####
##########################

"""NestedIterator is a container for instructions that build columns"""
struct NestedIterator{T} <: AbstractArray{T, 1}
    get_index::Function
    column_length::Int64
    el_type::Type{T}
    one_value::Bool
    unique_val::Ref{T}
end
Base.length(ni::NestedIterator) = ni.column_length
Base.size(ni::NestedIterator) = (ni.column_length,)
Base.getindex(ni::NestedIterator, i) = ni.get_index(i)
Base.eachindex(ni::NestedIterator) = 1:length(ni)


Base.collect(x::NestedIterator, pool_arrays) = pool_arrays ? PooledArray(x) : Vector(x)

abstract type InstructionCapture <: Function end

struct Seed{T} <: InstructionCapture
    data::T
end
(s::Seed)(i) = s.data[i]

struct UnrepeatEach <: InstructionCapture
    n::Int64
end
(u::UnrepeatEach)(i) = ceil(Int64, i/u.n)

"""repeat_each(c, N) will return an array where each source element appears N times in a row"""
function repeat_each(c::NestedIterator{T}, n) where T
    # when there is only one unique value, we can skip composing the repeat_each step
    return if c.one_value
        NestedIterator(c.get_index, c.column_length * n, T, true, c.unique_val)
    else
        NestedIterator(c.get_index ∘ UnrepeatEach(n), c.column_length * n, T, false, c.unique_val)
    end
end

struct Uncycle <: InstructionCapture
    n::Int64
end
(u::Uncycle)(i) = mod((i-1),u.n) + 1
"""cycle(c, n) cycles through an array N times"""
function cycle(c::NestedIterator{T}, n) where T
    # when there is only one unique value, we can skip composing the uncycle step
    return if c.one_value && !(typeof(c.get_index) <: Seed)
        NestedIterator(c.get_index, c.column_length * n, T, true, c.unique_val)
    else
        l = length(c)
        NestedIterator(c.get_index ∘ Uncycle(l), c.column_length * n, T, false, c.unique_val)
    end
end


struct Unstack{F, G} <: InstructionCapture
    f_len::Int64
    f::F
    g::G 
end
(u::Unstack)(i) = i > u.f_len ? u.g(i-u.f_len) : u.f(i)

"""stack(c1::NestedIterator, c2::NestedIterator)
Return a single NestedIterator which is the result of vcat(c1,c2)
"""
function stack(c1::NestedIterator{T}, c2::NestedIterator{U}) where {T, U}
    type = Union{T, U}
    len = (c1,c2) .|> length |> sum

    if T <: U
        only_one_value = c1.one_value && c2.one_value && isequal(c1.unique_val[], c2.unique_val[])
        if only_one_value
            return NestedIterator(c1.get_index, len, type, true, c1.unique_val)
        end
    end
    NestedIterator(Unstack(length(c1), c1.get_index, c2.get_index), len, type, false, Ref{type}())
end

stack(c) = c


"""
    NestedIterator(data; total_length=nothing)

Construct a new NestedIterator seeded with the value data
# Args
data::Any: seed value
total_length::Int: Cycle the values to reach total_length (must be even divisible by the length of `data`)
"""
function NestedIterator(data::T; total_length::Int=0, default_value=missing) where T
    value = if T <: AbstractArray
        length(data) == 0 ? (default_value,) : data
    else
        (data,)
    end
    len = length(value)
    ncycle = total_length < 1 ? 1 : total_length ÷ len
    return _NestedIterator(value, len, ncycle)
end

function _NestedIterator(value::T, len::Int64, ncycle::Int64) where T
    E = eltype(T)
    f = Seed(value)
    is_one = len == 1
    unique_val = Ref{E}()
    if is_one
        unique_val[] = first(value)::E
    end
    ni = NestedIterator{E}(f, len, E, is_one, unique_val)
    return cycle(ni, ncycle)
end


missing_column(default, len=1) = return NestedIterator(default; total_length=len)


##### ColumnDefinition #####
############################

"""ColumnDefinition provides a mechanism for specifying details for extracting data from a nested data source"""
mutable struct ColumnDefinition
    # Path to values
    field_path::Tuple
    # name of this column in the table once expanded
    const column_name::Symbol
    const default_value
    const pool_arrays::Bool
end
# Convenience alias
ColumnDefs = Vector{ColumnDefinition}

"""
    ColumnDefinition(field_path; column_name=nothing, flatten_arrays=false, default_value=missing, pool_arrays=false)

## Args
* `field_path`: Vector of keys/fieldnames that constitute a path from the top of the data to the values to extract for the column

## Keyword Args
* `column_name::Symbol`: A name for the resulting column. If `nothing`, defaults to joining the field_path with snake_case_format.
* `flatten_arrays::Bool`: When a leaf node is an array, should the values be flattened into separate rows or treated as a single value. Default: `true`
* `default_value`: When the field_path keys do not exist on one or more branches, fill with this value. Default: `missing`
* `pool_arrays::Bool`: When collecting values for this column, choose whether to use `PooledArrays` instead of `Base.Vector`. Default: `false` (use `Vector`)
* `name_join_pattern::String`: The separator for joining field paths into column names. Default: "_"
## Returns
`::ColumnDefinition`
"""
function ColumnDefinition(field_path; column_name=nothing, default_value=missing, pool_arrays=false, name_join_pattern::String = "_")
    if column_name isa Nothing
        path = last(field_path) == :unnamed ? field_path[1:end-1] : field_path
        column_name = join_names(path, name_join_pattern)
    end
    ColumnDefinition(field_path, column_name, default_value, pool_arrays)
end
function ColumnDefinition(field_path, column_names::Dict; pool_arrays::Bool, name_join_pattern = "_")
    column_name = field_path in keys(column_names) ? column_names[field_path] : nothing
    ColumnDefinition(field_path; column_name=column_name, pool_arrays=pool_arrays, name_join_pattern = name_join_pattern)
end
# Accessors
field_path(c::ColumnDefinition) = c.field_path
column_name(c::ColumnDefinition) = c.column_name
default_value(c::ColumnDefinition) = c.default_value
pool_arrays(c::ColumnDefinition) = c.pool_arrays
function current_path_name(c::ColumnDefinition, depth)
    fp = field_path(c)
    return fp[depth]
end
function path_to_children(c::ColumnDefinition, current_index)
    fp = field_path(c)
    return fp[current_index:end]
end


is_current_name(col_def::ColumnDefinition, name, depth) = current_path_name(col_def, depth) == name
has_more_keys(col_def, depth) = depth < length(field_path(col_def))
get_unique_current_names(defs, depth) = unique((current_path_name(def, depth) for def in defs))
function append_name!(def, name)
    new_field_path = tuple(field_path(def)..., name)
    def.field_path = new_field_path
    return def
end

function get_unique_names_and_children(col_defs::ColumnDefs, depth)
    unique_names = get_unique_current_names(col_defs, depth)
    names_with_children = get_unique_current_names(
        (def for def in col_defs if has_more_keys(def, depth)),
        depth
        )
    return (unique_names, names_with_children)
end

function make_column_def_child_copies(column_defs::ColumnDefs, name, depth)
    return filter(
        def -> is_current_name(def, name, depth) && length(field_path(def)) > depth, 
        column_defs
        )
end


##### ColumnSet #####
#####################

# Convenience alias for a dictionary of columns
ColumnSet = Dict{Tuple, NestedIterator} 
columnset(col, depth) = ColumnSet(Tuple(() for _ in 1:depth) => col)
init_column_set(data, depth) = columnset(NestedIterator(data), depth)
function init_column_set(data, name, depth)
    col_set = init_column_set(data, depth)
    prepend_name!(col_set, name, depth)
    return col_set
end
column_length(cols) = cols |> values |> first |> length 
# Add a name to the front of all names in a set of columns
function apply_in_place!(cols, f, args...)
    initial_keys = copy(keys(cols))
    for key in initial_keys 
        val = pop!(cols, key)
        key, val = f(key, val, args...)
        cols[key] = val
    end
end
function _prepend_name(key, val, name, depth)
    new_key = Tuple(i==depth ? name : k for (i,k) in enumerate(key))
    return new_key, val
end
function prepend_name!(cols, name, depth)
    depth < 1 && return nothing
    apply_in_place!(cols, _prepend_name, name, depth)
end

function _repeat_each_column(key, val, n)
    return key, repeat_each(val, n)
end
function repeat_each_column!(cols, n)
    apply_in_place!(cols,_repeat_each_column, n)
end


# Check if all the columns in a set are of equal length
all_equal_length(cols) = cols |> values .|> length |> allequal

"""
get_column(cols::ColumnSet, name, default=missing)

Get a column from a set with a given name, if no column with that name is found
construct a new column with same length as column set
"""
get_column(cols::ColumnSet, name, default=missing) = name in keys(cols) ? cols[name] : NestedIterator(default; total_length = column_length(cols))
# todo this assumse that default column divible into the length of the main column. that only works
# for length 1... so we should probably test this at the top
get_column(cols::ColumnSet, name, default::NestedIterator) = name in keys(cols) ? cols[name] : cycle(default, column_length(cols) ÷ length(default))

"""
column_set_product!(cols::ColumnSet)
Repeat values of all columns such that the resulting columns have every product of
the input columns. i.e.
column_set_product!(
    Dict(
        [:a] => [1,2],
        [:b] => [3,4,5]
    )
)
returns
Dict(
    [:a] => [1,1,1,2,2,2],
    [:b] => [3,4,5,3,4,5]
)
"""
function column_set_product!(cols::ColumnSet)
    multiplier = 1
    for (key, child_column) in pairs(cols)
        cols[key] = repeat_each(child_column, multiplier)
        multiplier *= length(child_column)
    end
    cols = cycle_columns_to_length!(cols)
    return cols
end


"""
cycle_columns_to_length!(cols::ColumnSet) 

Given a column set where the length of all columns is some factor of the length of the longest
column, cycle all the short columns to match the length of the longest
"""
function cycle_columns_to_length!(cols::ColumnSet)
    col_lengths = cols |> values .|> length
    longest = col_lengths |> maximum
    for (key, child_column) in pairs(cols)
        catchup_mult = longest ÷ length(child_column)
        cols[key] = cycle(child_column, catchup_mult)
    end
    return cols
end

"""Return a missing column for each member of a ColumnDefs"""
function make_missing_column_set(col_defs::ColumnDefs, current_index)
    missing_column_set =  Dict(
        path_to_children(def, current_index) => NestedIterator(default_value(def))
        for def in col_defs
    )
    return missing_column_set
end

function repeat_each!(column_set::ColumnSet, n)
    for (k, v) in pairs(column_set)
        columnset[k] = repeat_each(v, n)
    end
end


##### PathGraph #####
#####################

abstract type AbstractPathNode end
abstract type AbstractValueNode <: AbstractPathNode end

struct TopLevelNode <: AbstractPathNode
    children::Vector{AbstractPathNode}
end

struct PathNode <: AbstractPathNode
    name
    children::Vector{AbstractPathNode}
end

struct ValueNode <: AbstractValueNode
    name
    children::Vector{AbstractPathNode}
    field_path::Tuple
    pool_arrays
end
ValueNode(name, field_path, pool_arrays) = ValueNode(name, ValueNode[], field_path, pool_arrays)

children(n::AbstractPathNode) = n.children
name(n::AbstractPathNode) = n.name
field_path(n::AbstractValueNode) = n.field_path
pool_arrays(n::AbstractValueNode) = n.pool_arrays

"""
SIDE EFFECT: also appends :unnamed to any column defs that stop at a pathnode to capture any
loose values in an array at that level
"""
function make_path_nodes!(column_defs, depth = 1)
    # todo: cases where we are filtering down to a "current name" might be 
    # better as some kind of grouping
    unique_names = get_unique_current_names(column_defs, depth)
    nodes = Vector{AbstractPathNode}(undef, length(unique_names))
    for (i, unique_name) in enumerate(unique_names)
        matching_defs = filter(p -> current_path_name(p, depth) == unique_name, column_defs)
        are_value_nodes = [!has_more_keys(def, depth) for def in matching_defs]
        
        all_value_nodes = all(are_value_nodes)
        mix_of_node_types = !all_value_nodes && any(are_value_nodes)
        # todo turn this into a warning
        # if mix_of_node_types
        #     throw(ArgumentError("The path name $unique_name refers a value field in one branch and to nested child(ren) fields in another: $(field_path.(children_col_defs))"))
        # end

        if all_value_nodes
            # If we got to a value node, there should only be one.
            def = first(matching_defs)
            nodes[i] = ValueNode(unique_name, field_path(def), pool_arrays(def))
            continue
        end

        with_children = !mix_of_node_types ? 
            matching_defs :
            [def for (is_value, def) in zip(are_value_nodes, matching_defs) if !is_value]
        children_col_defs = make_column_def_child_copies(with_children, unique_name, depth)

        child_nodes = make_path_nodes!(children_col_defs, depth+1)
        if mix_of_node_types
            without_child_idx = findfirst(identity, are_value_nodes)
            without_child = matching_defs[without_child_idx]
            value_column_node = ValueNode(:unnamed, (field_path(without_child)..., :unnamed),pool_arrays(without_child))
            push!(child_nodes, value_column_node)
            append_name!(without_child, :unnamed)
        end

        nodes[i] = PathNode(unique_name, child_nodes)
    end
    return nodes
end 


"""Create a graph of field_paths that models the structure of the nested data"""
make_path_graph(col_defs::ColumnDefs) = TopLevelNode(make_path_nodes!(col_defs))


