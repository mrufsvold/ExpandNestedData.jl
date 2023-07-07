module NestedIterators
using PooledArrays
using SumTypes
using Compat
using ..NameLists: NameID, no_name_id
import ..get_name
import ..get_id
export RawNestedIterator, NestedIterator, seed, repeat_each, cycle, NestedVcat

@sum_type IterCapture :hidden begin
    RawSeed(::NameID)
    RawRepeat(::Int64)
    RawCycle(::Int64)
    RawVcat(::Int64, ::Vector{IterCapture}, ::Vector{IterCapture})
end

mutable struct RawNestedIterator
    # todo use linked list 
    get_index::Vector{IterCapture}
    column_length::Int64
    el_type::Type
    one_value::Bool
    unique_val::NameID
end
"""
RawNestedIterator(csm, data; total_length=nothing, default_value=missing)

Construct a new RawNestedIterator seeded with the value data
# Args
csm::ColumnSetManager
data::Any: seed value
total_length::Int: Cycle the values to reach total_length (must be even divisible by the length of `data`)
default_value: Value to fill if data is empty
"""
function RawNestedIterator(csm, data::T; total_length::Int=0, default_value=missing) where T
    value = if T <: AbstractArray
        length(data) == 0 ? (default_value,) : data
    else
        (data,)
    end
    is_one = allequal(value)
    len = length(value)
    val_T = typeof(value)
    id = get_id(csm, value)
    ncycle = total_length == 0 ? 1 : total_length ÷ len
    return RawNestedIterator(id, val_T, is_one, len, ncycle)
end

function RawNestedIterator(value_id::NameID, ::Type{T}, is_one::Bool, len::Int64, ncycle::Int64) where T
    E = eltype(T)
    f = IterCapture[IterCapture'.RawSeed(value_id), IterCapture'.RawCycle(len)]
    unique_val = is_one ? value_id : no_name_id
    return RawNestedIterator(f, len*ncycle, E, is_one, unique_val)
end
RawNestedIterator() = RawNestedIterator(IterCapture[], 0, Union{}, false, no_name_id)

Base.length(rni::RawNestedIterator) = rni.column_length
Base.size(rni::RawNestedIterator) = (rni.column_length,)
Base.collect(rni::RawNestedIterator, csm) = collect(NestedIterator(csm, rni))
Base.isequal(::RawNestedIterator, ::RawNestedIterator) = throw(ErrorException("To compare RawNestedIterator with RawNestedIterators, you must pass a ColumnSetManager."))
function Base.isequal(rni1::RawNestedIterator, rni2::RawNestedIterator, csm)
    rni1.column_length == rni2.column_length || return false
    rni1.el_type === rni2.el_type || return false
    if rni1.one_value != rni2.one_value
        return false
    else
        rni1.unique_val == rni2.unique_val && return true
        # if one iter was seeded with (1,) but the other was seeded with [1], the unique_val ids
        # will be different, so we need to check the actual values
        isequal(first(get_name(csm, rni1.unique_val)), first(get_name(csm,rni2.unique_val))) && return true
    end
    return isequal(collect(rni1, csm), collect(rni2, csm))
end

abstract type InstructionCapture <: Function end

"""Seed is the core starter for a NestedIterator"""
struct Seed{T} <: InstructionCapture
    data::T
end
(s::Seed)(i) = s.data[i]
struct RawSeed
    data_id::NameID 
end
Seed(csm, raw_seed::RawSeed) = get_name(csm, raw_seed.data_id)

"""Captures the repeat value for a repeat_each call"""
struct UnrepeatEach <: InstructionCapture
    n::Int64
end
(u::UnrepeatEach)(i) = ceil(Int64, i/u.n)

function repeat_each(c::RawNestedIterator, n)
    # when there is only one unique value, we can skip composing the repeat_each step
    c.column_length *= n
    if c.one_value
        return c
    end
    push!(c.get_index,IterCapture'.RawRepeat(n))
    return c
end

"""Captures the repeat value for a cycle call"""
struct Uncycle <: InstructionCapture
    n::Int64
end
(u::Uncycle)(i) = mod((i-1),u.n) + 1
"""cycle(c, n) cycles through an array N times"""
function cycle(c::RawNestedIterator, n)
    original_len = c.column_length
    # when there is only one unique value, we can skip composing the uncycle step
    c.column_length *= n
    if c.one_value
        return c
    end
    push!(c.get_index,IterCapture'.RawCycle(original_len))
    return c
end

"""Captures the two getindex functions of vcated NestedIterators. f_len tells which index to break over to g."""
struct Unvcat{F, G} <: InstructionCapture
    f_len::Int64
    f::F
    g::G 
end
(u::Unvcat)(i) = i > u.f_len ? u.g(i-u.f_len) : u.f(i)

"""vcat(csm::ColumnSetManger, c1::RawNestedIterator, c2::RawNestedIterator)
Return a single NestedIterator which is the result of vcat(c1,c2)
"""
function _vcat(csm, c1::RawNestedIterator, c2::RawNestedIterator)
    c1_len = length(c1)
    c2_len = length(c2)
    c1_len == 0 && return c2
    c2_len == 0 && return c1
    
    T1 = c1.el_type
    T2 = c2.el_type
    only_one_value = if T1 === T2 && c1.one_value && c2.one_value
        v1 = get_single_value(csm, c1.unique_val, T1)
        v2 = get_single_value(csm, c2.unique_val, T1)
        isequal(v1, v2)
    else
        false
    end

    type = Union{T1, T2}
    len = c1_len + c2_len

    if only_one_value
        c1.column_length = len
        return c1
    end
    
    return RawNestedIterator(
        IterCapture[IterCapture'.RawVcat(c1_len, c1.get_index, c2.get_index)], 
        len, type, false, no_name_id
    )
end
get_single_value(csm, id, ::Type{T}) where T = first(get_name(csm, id))::T

struct NestedVcat{T} <: Function
    csm::T
end
(v::NestedVcat)(c1,c2) = _vcat(v.csm, c1, c2)
(v::NestedVcat)(c1) = c1

function build_get_index(csm, captures)
    iter_funcs = Iterators.map(cap -> get_iter_func(csm, cap), captures)
    return foldr(∘, iter_funcs)
end

function get_iter_func(csm, capture::IterCapture)
    @cases capture begin
        RawSeed(id) => Seed(get_name(csm, id))
        RawRepeat(n) => UnrepeatEach(n)
        RawCycle(n) => Uncycle(n)
        RawVcat(len, iter1, iter2) => Unvcat(len, build_get_index(csm, iter1), build_get_index(csm, iter2))
    end
end

"""NestedIterator is a container for instructions that build columns"""
struct NestedIterator{T,F} <: AbstractArray{T, 1}
    get_index::F
    column_length::Int64
    el_type::Type{T}
    function NestedIterator(get_index, column_length, el_type)
        return new{el_type, typeof(get_index)}(get_index, column_length, el_type)
    end
end
function NestedIterator(csm, raw::RawNestedIterator)
    get_index = build_get_index(csm, raw.get_index)
    return NestedIterator(get_index, length(raw), raw.el_type)
end
Base.length(ni::NestedIterator) = ni.column_length
Base.size(ni::NestedIterator) = (ni.column_length,)
Base.getindex(ni::NestedIterator, i) = ni.get_index(i)
Base.eachindex(ni::NestedIterator) = 1:length(ni)
Base.collect(x::NestedIterator, pool_arrays=false) = pool_arrays ? PooledArray(x) : Vector(x)

end #NestedIterators
