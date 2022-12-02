##### NameValueContainer #####
##############################

"""NameValueContainer is an abstraction on Dict and DataType structs so that we can get their
contents without worrying about `getkey` or `getproperty`, etc.
"""
NameValueContainer = Union{StructTypes.DictType, StructTypes.DataType}

is_NameValueContainer(t) = typeof(StructTypes.StructType(t)) <: NameValueContainer

"""Check if any elements in an iterator are subtypes of NameValueContainer"""
function has_namevaluecontainer_element(itr)
    if eltype(itr) == Any
        return itr .|> eltype .|> is_NameValueContainer |> any
    else
        return itr |> eltype |> get_member_types .|> is_NameValueContainer |> any
    end
end
get_member_types(T) = T isa Union ? Base.uniontypes(T) : [T]

"""Define a pairs iterator for all DataType structs"""
get_pairs(x::T) where T = get_pairs(StructTypes.StructType(T), x)
get_pairs(::StructTypes.DataType, x) = ((p, getproperty(x, p)) for p in fieldnames(typeof(x)))
get_pairs(::StructTypes.DictType, x) = pairs(x)

"""Get the keys/names of any NameValueContainer"""
get_names(x::T) where T = get_names(StructTypes.StructType(T), x)
get_names(::StructTypes.DataType, x) = (n for n in fieldnames(typeof(x)))
get_names(::StructTypes.DictType, x) = keys(x)

get_value(x::T, name) where T = get_value(StructTypes.StructType(T), x, name)
get_value(::StructTypes.DataType, x, name) = getproperty(x, name)
get_value(::StructTypes.DictType, x, name) = x[name]


##### NestedIterator #####
##########################

"""NestedIterator is a container for instructions that build columns"""
mutable struct NestedIterator{T} <: AbstractArray{T, 1}
    get_index::Function
    column_length::Int64
    unique_values::Set{T}
end
Base.length(ni::NestedIterator) = ni.column_length
Base.size(ni::NestedIterator) = (ni.column_length,)
Base.getindex(ni::NestedIterator, i) = ni.get_index(i)
Base.eachindex(ni::NestedIterator) = 1:length(ni)


Base.collect(x::NestedIterator, pool_arrays) = pool_arrays && !(x.unique_values isa Nothing) ? PooledArray(x) : Vector(x)


"""repeat_each!(c, N) will return an array where each source element appears N times in a row"""
function repeat_each!(c::NestedIterator, n)
    # when there is only one unique value, we can skip composing the unrepeat_each step
    if length(c.unique_values) != 1
        c.get_index = c.get_index ∘ ((i) -> unrepeat_each(i, n))
    end
    c.column_length *= n
end
unrepeat_each(i, n) = ceil(Int64, i/n)


"""cycle!(c, n) cycles through an array N times"""
function cycle!(c::NestedIterator, n)
    # when there is only one unique value, we can skip composing the uncycle step
    if length(c.unique_values) != 1
        l = length(c)
        c.get_index = c.get_index ∘ ((i::Int64) -> uncycle(i, l))
    end
    c.column_length *= n
end
uncycle(i,n) = mod((i-1),n) + 1

"""stack(c1::NestedIterator, c2::NestedIterator)
Return a single NestedIterator which is the result of vcat(c1,c2)
"""
function stack(c1::NestedIterator, c2::NestedIterator)
    type = Union{eltype(c1), eltype(c2)}
    len = (c1,c2) .|> length |> sum

    continue_tracking_uniques = 0 < length(c1.unique_values) < 100 &&
                                0 < length(c2.unique_values) < 100
    values = continue_tracking_uniques ? union(c1.unique_values, c2.unique_values) : Set{type}([])

    f = length(values) == 1 ?
        c1.get_index :
        ((i::Int64) -> unstack(i, length(c1), c1.get_index, c2.get_index))
    
    return NestedIterator{type}(f, len, values)
end
unstack(i::Int64, c1_len::Int64, f1::Function, f2::Function) = i > c1_len ? f2(i-c1_len) : f1(i)


"""
NestedIterator(data, flatten_arrays=true)
Construct a new NestedIterator seeded with the value data
# Args
data::Any: seed value
flatten_arrays::Bool: if data is an array, flatten_arrays==false will treat the array as a single value when 
    cycling the columns values
"""
function NestedIterator(data; flatten_arrays=false, total_length=nothing, default_value=missing)
    value = if flatten_arrays && typeof(data) <: AbstractArray
        length(data) > 1 ? data : [default_value]
    else
        [data]
    end

    len = length(value)
    type = eltype(value)
    f = len == 1 ? ((::Int64) -> value[1]) : ((i::Int64) -> value[i])
    ni = NestedIterator{type}(f, len, Set(value))
    if !(total_length isa Nothing)
        cycle!(ni, total_length)
    end
    return ni
end


function missing_column(default, len=1)
    col = NestedIterator(default)
    cycle!(col, len)
    return col
end


##### ColumnDefinition #####
############################

"""ColumnDefinition provides a mechanism for specifying details for extracting data from a nested data source"""
struct ColumnDefinition
    field_path
    path_index::Int64
    column_name::Symbol
    flatten_arrays::Bool
    default_value
    pool_arrays::Bool
end
# Convenience alias
ColumnDefs = Vector{ColumnDefinition}


"""
    ColumnDefinition(field_path; column_name=nothing, flatten_arrays=false, default_value=missing, pool_arrays=false)

## Args
* `field_path`: Vector of keys/fieldnames that constitute a path from the top of the data to the values to extract for the column

## Keyword Args
* `column_name::Symbol`: A name for the resulting column. If `nothing`, defaults to joining the field_path with snake_case_format.
* `flatten_arrays`: When a leaf node is an array, should the values be flattened into separate rows or treated as a single value. Default: `true`
* `default_value`: When the field_path keys do not exist on one or more branches, fill with this value. Default: `missing`
* `pool_arrays`: When collecting values for this column, choose whether to use `PooledArrays` instead of `Base.Vector`. Default: `false` (use `Vector`)

## Returns
`::ColumnDefinition`
"""
function ColumnDefinition(field_path; column_name=nothing, flatten_arrays=false, default_value=missing, pool_arrays=false)
    column_name = column_name isa Nothing ? join_names(field_path) : column_name
    ColumnDefinition(field_path, 1, column_name, flatten_arrays, default_value, pool_arrays)
end
function ColumnDefinition(field_path, column_names::Dict)
    column_name = field_path in keys(column_names) ? column_names[field_path] : nothing
    ColumnDefinition(field_path; column_name=column_name)
end
# Accessors
field_path(c::ColumnDefinition) = c.field_path
column_name(c::ColumnDefinition) = c.column_name
default_value(c::ColumnDefinition) = c.default_value
pool_arrays(c::ColumnDefinition) = c.pool_arrays
flatten_arrays(c::ColumnDefinition) = c.flatten_arrays
path_index(c::ColumnDefinition) = c.path_index
function current_path_name(c::ColumnDefinition)
    fp = field_path(c)
    i = path_index(c)
    return fp[i]
end


is_current_name(col_def::ColumnDefinition, name) = current_path_name(col_def) == name

has_more_keys(col_def) = path_index(col_def) < length(field_path(col_def))


function analyze_column_defs(col_defs::ColumnDefs)
    unique_names = col_defs .|> current_path_name |> unique
    names_with_children = filter(has_more_keys, col_defs) .|> current_path_name |> unique
    return (unique_names, names_with_children)
end

function make_column_def_child_copies(column_defs::ColumnDefs, name)
    return filter((def -> is_current_name(def, name)), column_defs) .|>
        (def -> ColumnDefinition(
            field_path(def),
            path_index(def) + 1,
            column_name(def),
            flatten_arrays(def),
            default_value(def),
            pool_arrays(def)
        ))
end


##### ColumnSet #####
#####################

# Convenience alias for a dictionary of columns
ColumnSet = Dict{N where N <: Union{Symbol,Vector{Symbol}}, NestedIterator{T} where T <: Any} 
columnset(col) = ColumnSet(Symbol[] => col)
init_column_set(data, flatten_arrays=true) = columnset(NestedIterator(data; flatten_arrays))
column_length(cols) = cols |> values |> first |> length 
# Add a name to the front of all names in a set of columns
prepend_name!(cols, name) = cols |> keys .|> (k-> pushfirst!(k, name))
# Check if all the columns in a set are of equal length
all_equal_length(cols) = cols |> values .|> length |> allequal

"""
get_column(cols::ColumnSet, name, default=missing)

Get a column from a set with a given name, if no column with that name is found
construct a new column with same length as column set
"""
get_column(cols::ColumnSet, name, default=missing) = name in keys(cols) ? cols[name] : NestedIterator(default; total_length = column_length(cols))


"""
column_set_product!(cols::ColumnSet)
Repeat values of all columns such that the resulting columns have every product of
the input columns. i.e.
column_set_product!(
    Dict(
        [:a] => [1,2],
        [:b] =? [3,4,5]
    )
)
returns
Dict(
    [:a] => [1,1,1,2,2,2],
    [:b] =? [3,4,5,3,4,5]
)
"""
function column_set_product!(cols::ColumnSet)
    multiplier = 1
    for child_column in values(cols)
        repeat_each!(child_column, multiplier)
        multiplier *= length(child_column)
    end
    cycle_columns_to_length!(cols)
end


"""
cycle_columns_to_length!(cols::ColumnSet) 

Given a column set where the length of all columns is some factor of the length of the longest
column, cycle all the short columns to match the length of the longest
"""
function cycle_columns_to_length!(cols::ColumnSet)
    longest = cols |> values .|> length |> maximum
    for child_column in values(cols)
        catchup_mult = Int(longest / length(child_column))
        cycle!(child_column, catchup_mult)
    end
end

"""Return a missing column for each member of a ColumnDefs"""
function make_missing_column_set(child_col_defs::ColumnDefs)
    missing_column_set =  Dict(
        column_name(def) => NestedIterator(default_value(def))
        for def in child_col_defs
    )
    return missing_column_set
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
    column_name
end

children(n::AbstractPathNode) = n.children
name(n::AbstractPathNode) = n.name

function make_path_nodes(column_defs)
    unique_names = column_defs .|> current_path_name |> unique
    nodes = Vector{AbstractPathNode}(undef, length(unique_names))
    for (i, unique_name) in enumerate(unique_names)
        matching_defs = filter(p -> current_path_name(p) == unique_name, column_defs)
        are_value_nodes = matching_defs .|> has_more_keys .|> !

        if all(are_value_nodes)
            # If we got to a value node, there should only be one.
            col = first(matching_defs)
            nodes[i] = ValueNode(unique_name, column_name(col))
            continue
        end

        children_col_defs = matching_defs .|> make_column_def_child_copies
        if any(are_value_nodes)
            throw(ArgumentError("The path name $unique_name refers a value in one branch and to nested child(ren): $(field_path.(children_names))"))
        end
        nodes[i] = PathNode(unique_name, make_path_nodes(children_col_defs))
    end
    return nodes
end 


"""Create a graph of field_paths that models the structure of the nested data"""
make_path_graph(col_defs::ColumnDefs) = TopLevelNode(make_path_nodes(col_defs))
make_path_graph(paths, column_names::Dict = Dict()) = make_path_graph(ColumnDefinition.(paths, Ref(column_names)))


