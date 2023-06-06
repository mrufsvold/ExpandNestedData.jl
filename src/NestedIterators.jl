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
Base.collect(x::NestedIterator, pool_arrays=false) = pool_arrays ? PooledArray(x) : Vector(x)

abstract type InstructionCapture <: Function end

"""Seed is the core starter for a NestedIterator"""
struct Seed{T} <: InstructionCapture
    data::T
end
(s::Seed)(i) = s.data[i]

"""Captures the repeat value for a repeat_each call"""
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

"""Captures the repeat value for a cycle call"""
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

"""Captures the two getindex functions of stacked NestedIterators. f_len tells which index to break over to g."""
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
