module NormalizeDict
using PooledArrays
using StructTypes
using Logging
import Base.Iterators: repeated, flatten

NameValuePairsObject = Union{StructTypes.DictType, StructTypes.DataType}
is_NameValuePairsObject(t) = typeof(StructTypes.StructType(t)) <: NameValuePairsObject

"""Check if any elements in an iterator are subtypes of NameValuePairsObject"""
any_name_value_pair_els(itr) = els .|> StructTypes.StructType .|> (st -> st <: NameValuePairsObject) |> any

"""Define a pairs iterator for all DataType structs"""
get_pairs(x::T) where T = get_pairs(StructTypes.StructType(T), x)
get_pairs(::StructTypes.DataType, x) = ((p, getproperty(x, p)) for p in fieldnames(typeof(x)))
get_pairs(::StructTypes.DictType, x) = pairs(x)

"""Get the keys/names of any NameValuePairsObject object"""
get_names(x::T) where T = get_names(StructTypes.StructType(T), x)
get_names(::StructTypes.DataType, x) = (n for n in fieldnames(typeof(x)))
get_names(x::Dict) = keys(x)


# Instructions are steps that need to be taken to construct a column
abstract type AbstractInstruction end

"""NestedIterator is a container for instructions that build columns"""
mutable struct NestedIterator{T} <: AbstractArray{T, 1}
    get_index::Function
    column_length::Int64
    unique_element_count::Int64
    #todo add an "element number" field to store the number of unique values
end
Base.length(ni::NestedIterator) = ni.column_length
Base.size(ni::NestedIterator) = (ni.column_length,)
Base.getindex(ni::NestedIterator, i) = ni.get_index(i)
Base.eachindex(ni::NestedIterator) = 1:length(ni)


Base.collect(x::NestedIterator, use_pool) = use_pool && x.unique_element_count > 0 ? PooledArray(x) : Vector(x)

# Get the steps from the NestedIterator object
update_length!(col::NestedIterator, i::Int) = (col.column_length = i)

"""repeat_each!(c, N) will return an array where each source element appears N times in a row"""
function repeat_each!(c::NestedIterator, n)
    f = ((i) -> unrepeat_each(i, n))
    c.get_index = c.get_index ∘ f
    c.column_length *= n
end
unrepeat_each(i, n) = ceil(Int64, i/n)

"""cycle!(c, n) cycles through an array N times"""
function cycle!(c::NestedIterator, n)
    l = length(c)
    f = ((i) -> uncycle(i, l))
    c.get_index = c.get_index ∘ f
    c.column_length *= n
end
uncycle(i,n) = mod((i-1),n) + 1

unstack(i, c1_len, f1, f2) = i > c1_len ? f2(i-c1_len) : f1(i)
function stack(c1::NestedIterator, c2::NestedIterator)
    type = Union{eltype(c1), eltype(c2)}
    len = (c1,c2) .|> length |> sum

    bigger_el_count = max(c1.unique_element_count, c2.unique_element_count)

    total_unique_elements = if bigger_el_count < 100 || bigger_el_count < len*0.1
        new_el_count = sum(skipmissing(!(el in c1) for el in c2);init=0)
        c1.unique_element_count + new_el_count
    else
        0
    end
    f = (i) -> unstack(i, length(c1), c1.get_index, c2.get_index)
    return NestedIterator{type}(f, len, total_unique_elements)
end

function init_column(data, expand_arrays=true)
    value = (expand_arrays && typeof(data) <: AbstractArray) ? data : [data]
    len = length(value)
    type = eltype(value)
    f = (i::Int64) -> value[i]
    return NestedIterator{type}(f, len, len)
end

function missing_column(default, len)
    col = init_column(default)
    cycle!(col, len)
    return col
end



# Convenience alias for a dictionary of columns
ColumnSet = Dict{Vector{Symbol}, NestedIterator}
columnset(col) = ColumnSet(Symbol[] => col)
init_column_set(data, expand_arrays=true) = columnset(init_column(data, expand_arrays))
column_length(cols::ColumnSet) = cols |> values |> first |> length 
# Add a name to the front of all names in a set of columns
prepend_name!(cols::ColumnSet, name) = cols |> keys .|> (k-> pushfirst!(k, name))
# Check if all the columns in a set are of equal length
all_equal_length(cols::ColumnSet) = cols |> values .|> length |> allequal
"""
get_column(cols::ColumnSet, name, default=missing)

Get a column from a set with a given name, if no column with that name is found
# construct a new column with same length as column set
"""
get_column(cols::ColumnSet, name, default=missing) = name in keys(cols) ? cols[name] : missing_column(default, column_length(cols))


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


process_node(data::T; kwargs...) where T = process_node(StructTypes.StructType(T), data; kwargs...)

# If we get an array type, check if it should be expanded further or if it should be the seed of a new column
function process_node(data::AbstractArray{T}; kwargs...) where {T}
    # In the following cases, keep decending the tree
    continue_processing = (
        # If expand_arrays is true
        kwargs[:expand_arrays] ||
        # Empty array doesn't need further expansion
        length(data) == 0 ||
        # If all of the elements are name-value pair objects
        is_NameValuePairsObject(T) ||
        # Or if the elements are a union of types and any of them are name-value pair objects
        (T <: Union && any_name_value_pair_els(Base.uniontypes(T) )) || 
        # or if the elements are Any, we just need to check each one for name-value pair necessary
        (T == Any && any_name_value_pair_els(data))
    )
    if continue_processing
        return process_node(StructTypes.ArrayType(), data; kwargs...)
    end

    return process_node(nothing, data; kwargs...)
end


"""
process_node(data::Any)

This is the base case for processing nodes. It creates a new column, seeds with `data`
and returns.
"""
function process_node(::Any, data; kwargs...)
    return init_column_set(data, kwargs[:expand_arrays])
end


"""
process_node(data::NameValuePairsObject)

For nodes that contain name-value pairs, process each value 
"""
function process_node(::D, data; kwargs...) where D <: NameValuePairsObject
    columns = ColumnSet()
    multiplier = 1
    for (child_name, child_data) in get_pairs(data)
        # Collect columns from the child's data
        child_columns = process_node(child_data; kwargs...)
        # Add the child's name to the key of all columns
        prepend_name!(child_columns, child_name)
        # Need to repeat each value for all of the values of the previous children
        # to make a product of values
        repeat_each!.(values(child_columns), multiplier)
        multiplier *= column_length(child_columns)
        merge!(columns, child_columns)
    end
    # catch up short columns with the total length for this group
    cycle_columns_to_length!(columns)

    return columns
end




function process_node(::A, data; kwargs...) where A <: StructTypes.ArrayType
    if length(data) == 0
        return columnset(missing_column(kwargs[:missing_value], 1))
    elseif  length(data) == 1
        return process_node(first(data); kwargs...)
    end

    all_column_sets = process_node.(data; kwargs...)

    unique_names = all_column_sets .|> keys |> Iterators.flatten |> unique

    column_set = ColumnSet()
    for name in unique_names
        column_set[name] = all_column_sets         .|>
            (col_set -> get_column(col_set, name, kwargs[:missing_value]))  |>
            (cols -> foldl(stack, cols))
    end
    return column_set
end


function normalize(data; expand_arrays::Bool = false, missing_value = missing, use_pool = false)
    @info "normalizing data"
    columns = process_node(data; expand_arrays=expand_arrays, missing_value=missing_value)
    @info "Retrieved $(length(columns)) columns from the data. They are $(column_length(columns)) elements long"
    names = keys(columns)
    column_vecs = names .|> (n -> columns[n]) .|> (c -> collect(c, use_pool))
    @info "returning table"
    return NamedTuple{Tuple(join_names(n) for n in names)}(column_vecs)
end

join_names(names) = names .|> string |> (s -> join(s, "_")) |> Symbol

end
