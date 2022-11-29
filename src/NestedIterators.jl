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
