module NormalizeDict
using PooledArrays
using StructTypes
using Logging
import Base.Iterators: repeated, flatten

include("AbstractedGetters.jl")
include("NestedIterators.jl")

NameValuePairsObject = Union{StructTypes.DictType, StructTypes.DataType}
is_NameValuePairsObject(t) = typeof(StructTypes.StructType(t)) <: NameValuePairsObject

"""Check if any elements in an iterator are subtypes of NameValuePairsObject"""
any_name_value_pair_els(itr) = itr .|> StructTypes.StructType .|> (st -> st <: NameValuePairsObject) |> any


function normalize(data; expand_arrays::Bool = false, missing_value = missing, use_pool = false)
    @info "normalizing data"
    columns = process_node(data; expand_arrays=expand_arrays, missing_value=missing_value)
    @info "Retrieved $(length(columns)) columns from the data. They are $(column_length(columns)) elements long"
    names = keys(columns)
    column_vecs = names .|> (n -> columns[n]) .|> (c -> collect(c, use_pool))
    @info "returning table"
    return NamedTuple{Tuple(join_names(n) for n in names)}(column_vecs)
end

"""
process_node(data; kwargs...)
Recursively process a nested data structure. Return a Dict{Vector{Symbol}, NestedIterator}
where each Pair is a column that represents all values for a given set of keys/fieldnames.

Args:
data::Union{StructTypes.DictType, StructTypes.DataType}: A nested data structure
Kwargs:
expand_arrays::Bool: when a leaf node's values is an array, should it be flattened or left as a single value
missing_value::Bool: Default value if a certain key/fieldname path is missing along some legs of the data

Returns
Dict{Vector{Symbol}, NestedIterator}: All unique key/fieldname paths and an iterator of values for each path
"""
# Dispatch processing of an object to the correct version of process node using StructType
process_node(data::T; kwargs...) where T = process_node(StructTypes.StructType(T), data; kwargs...)


# Make a new column when you get to the bottom of the nested objects
process_node(::Any, data; kwargs...) = init_column_set(data, kwargs[:expand_arrays])


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


# handle unpacking arraylike objects
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


# Handle a name-value pair object
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


# Link a list of keys into an underscore separted column name
join_names(names) = names .|> string |> (s -> join(s, "_")) |> Symbol


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
construct a new column with same length as column set
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

end
