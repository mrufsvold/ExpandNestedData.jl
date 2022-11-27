# Instructions are steps that need to be taken to construct a column
abstract type AbstractInstruction end

"""NestedIterator is a container for instructions that build columns"""
mutable struct NestedIterator{T} <: AbstractArray{T, 1}
    get_index::Function
    column_length::Int64
    unique_values::Set{T}
    #todo add an "element number" field to store the number of unique values
end
Base.length(ni::NestedIterator) = ni.column_length
Base.size(ni::NestedIterator) = (ni.column_length,)
Base.getindex(ni::NestedIterator, i) = ni.get_index(i)
Base.eachindex(ni::NestedIterator) = 1:length(ni)


Base.collect(x::NestedIterator, use_pool) = use_pool && !(x.unique_values isa Nothing) ? PooledArray(x) : Vector(x)

# Get the steps from the NestedIterator object
update_length!(col::NestedIterator, i::Int) = (col.column_length = i)

"""repeat_each!(c, N) will return an array where each source element appears N times in a row"""
function repeat_each!(c::NestedIterator, n)
    if length(c.unique_values) == 1
        c.get_index = c.get_index ∘ ((i) -> unrepeat_each(i, n))
    end
    c.column_length *= n
end
unrepeat_each(i, n) = ceil(Int64, i/n)

"""cycle!(c, n) cycles through an array N times"""
function cycle!(c::NestedIterator, n)
    l = length(c)
    if length(c.unique_values) == 1
        c.get_index = c.get_index ∘ ((i::Int64) -> uncycle(i, l))
    end
    c.column_length *= n
end
uncycle(i,n) = mod((i-1),n) + 1

unstack(i::Int64, c1_len::Int64, f1::Function, f2::Function) = i > c1_len ? f2(i-c1_len) : f1(i)
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

function init_column(data, expand_arrays=true)
    value = (expand_arrays && typeof(data) <: AbstractArray) ? data : [data]
    len = length(value)
    type = eltype(value)
    f = len == 1 ? ((::Int64) -> value[1]) : ((i::Int64) -> value[i])
    return NestedIterator{type}(f, len, Set(value))
end

function missing_column(default, len)
    col = init_column(default)
    cycle!(col, len)
    return col
end
