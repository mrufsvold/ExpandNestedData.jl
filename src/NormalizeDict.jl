module NormalizeDict
using StructTypes
import Base.Iterators: repeated, flatten

NameValuePairsObject = Union{StructTypes.DictType, StructTypes.DataType}
is_NameValuePairsObject(t) = typeof(StructTypes.StructType(t)) <: NameValuePairsObject

"""Check if any elements in an iterator are subtypes of NameValuePairsObject"""
any_name_value_pair_els(itr) = els .|> StructTypes.StructType .|> (st -> st <: NameValuePairsObject) |> any

"""Define a pairs iterator for all DataType structs"""
get_pairs(x::T) where T = get_pairs(StructTypes.StructType(T), x)
get_pairs(::StructTypes.DataType, x) = ((p, getproperty(x, p)) for p in fieldnames(typeof(x)))
get_pairs(x::Dict) = pairs(x)

"""Get the keys/names of any NameValuePairsObject object"""
get_names(x::T) where T = get_names(StructTypes.StructType(T), x)
get_names(::StructTypes.DataType, x) = (n for n in fieldnames(typeof(x)))
get_names(x::Dict) = keys(x)


# Instructions are steps that need to be taken to construct a column
abstract type AbstractInstruction end

"""ColumnInstructions is a container for instructions that build columns"""
Base.@kwdef mutable struct ColumnInstructions
    steps::Channel{AbstractInstruction} = Channel{AbstractInstruction}(100)
    column_length::Int64 = 0
    el_type::Type = Any
end
column_length(c::ColumnInstructions) = c.column_length
# Get the steps from the ColumnInstructions object
steps(col::ColumnInstructions) = col.steps
el_type(col::ColumnInstructions) = col.el_type
set_el_type!(col::ColumnInstructions, t::Type) = (col.el_type = t)

"""Fallback function for adding a new Instruction to a ColumnInstructions"""
add_step!(c::ColumnInstructions, step::AbstractInstruction) = put!(c.steps, step)
"""Seed is the original value that starts a columns generator"""
struct Seed <: AbstractInstruction
    value
end
function add_step!(c::ColumnInstructions, s::Seed)
    set_el_type!(c, typeof(s.value))
    put!(steps(c), s)
end
apply(::Nothing, step::Seed) = step.value

"""Wrap all current values in an array so that it has only one "element" """
struct Wrap <: AbstractInstruction end
function add_step!(c::ColumnInstructions, step::Wrap)
    put!(steps(c), step)
    c.column_length = 1
end
apply(curr, ::Wrap) = [curr]

# Repeat instructions repeat all of the values in the column in some way
abstract type AbstractRepeatInstruction <: AbstractInstruction end
function add_step!(col::ColumnInstructions, s::AbstractRepeatInstruction)
    put!(steps(col), s)
    col.column_length *= s.value
end

"""RepeatEach(N) will return an array where each source element appears N times in a row"""
struct RepeatEach <: AbstractRepeatInstruction
    value
end
apply(curr, step::RepeatEach) = curr .|> (v -> repeated(v, step.value)) |> flatten
"""Cycle(N) cycles through an array N times"""
struct Cycle <: AbstractRepeatInstruction
    value
end
apply(curr, step::Cycle) = 1:step.value .|> (_ -> curr) |> flatten

struct Insert <: AbstractInstruction
    column
end
function add_step!(col::ColumnInstructions, s::Insert)
    put!(steps(col), s)
    col.column_length += column_length(s.column)
    set_el_type!(col, Union{el_type(col), el_type(s.column)}) 
end
apply(curr, s::Insert) = flatten((curr , make_generator(s.column)))

function stack(column1, columns2) 
    add_step!(column1, Insert(columns2))
    return column1
end

function init_column(data, wrap=true)
    col = ColumnInstructions()
    add_step!(col, Seed(data))
    wrap && add_step!(col, Wrap())
    return col
end

function missing_column(default, len)
    col = init_column(default)
    add_step!(col, Cycle(len))
    return col
end

function make_generator(c::ColumnInstructions)
    column_values = nothing 
    while isready(c.steps)
        step = take!(c.steps)
        column_values = apply(column_values, step)
    end
    return column_values
end

# Convenience alias for a dictionary of columns
ColumnSet = Dict{Vector{Symbol}, ColumnInstructions}
columnset(col) = ColumnSet(Symbol[] => col)
init_column_set(data) = columnset(init_column(data))
column_length(cols::ColumnSet) = cols |> values |> first |> column_length 
# Add the same step to all columns in a ColumnSet
add_step!(cols::ColumnSet, s) = cols |> values .|> (col -> add_step!(col, s))
# Add a name to the front of all names in a set of columns
prepend_name!(cols::ColumnSet, name) = cols |> keys .|> (k-> pushfirst!(k, name))
# Check if all the columns in a set are of equal length
all_equal_length(cols::ColumnSet) = cols |> values .|> column_length |> allequal
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
        add_step!(child_column, RepeatEach(multiplier))
        multiplier *= column_length(child_column)
    end
    cycle_columns_to_length!(cols)
end

"""
cycle_columns_to_length!(cols::ColumnSet) 

Given a column set where the length of all columns is some factor of the length of the longest
column, cycle all the short columns to match the length of the longest
"""
function cycle_columns_to_length!(cols::ColumnSet)
    longest = cols |> values .|> column_length |> maximum
    for child_column in values(cols)
        catchup_mult = Int(longest / column_length(child_column))
        add_step!(child_column, Cycle(catchup_mult))
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
    return init_column_set(data)
end


"""
process_node(data::NameValuePairsObject)

For nodes that contain name-value pairs, process each value 
"""
function process_node(::D, data; kwargs...) where D <: NameValuePairsObject
    columns = ColumnSet()
    multiplier = 1
    for (child_name, child_data) in pairs(data)
        # Collect columns from the child's data
        child_columns = process_node(child_data; kwargs...)
        # Add the child's name to the key of all columns
        prepend_name!(child_columns, child_name)
        # Need to repeat each value for all of the values of the previous children
        # to make a product of values
        add_step!(child_columns, RepeatEach(multiplier))
        merge!(columns, child_columns)
    end
    # catch up short columns with the total length for this group
    cycle_columns_to_length!(columns)
    return columns
end




function process_node(::A, data; kwargs...) where A <: StructTypes.ArrayType
    if length(data) == 0
        return columnset(missing_column(missing, 1))
    end
    if length(data) == 1
        return process_node(data; kwargs...)
    end

    all_column_sets = process_node.(data; kwargs...)

    unique_names = all_column_sets .|> keys |> Iterators.flatten |> unique

    column_set = ColumnSet()
    for name in unique_names
        column_set[name] = all_column_sets         .|>
            (col_set -> get_column(col_set, name))  |>
            (cols -> foldl(stack, cols))
    end
    return column_set
end


function normalize(data; expand_arrays::Bool = false)
    columns = process_node(data; expand_arrays=expand_arrays)
    names = keys(columns)
    column_vecs = Vector{Vector}(undef, length(columns))
    for (i, name) in enumerate(names)
        col_gen = make_generator(columns[name])
        vec = Vector{el_type(columns[name])}(undef, column_length(columns[name]))
        for (j, val) in enumerate(col_gen)
            vec[j] = val
        end
        column_vecs[i] = vec
    end
    return NamedTuple{Tuple(join_names(n) for n in names)}(column_vecs)
end

join_names(names) = names .|> string |> (s -> join(s, "_")) |> Symbol

end
