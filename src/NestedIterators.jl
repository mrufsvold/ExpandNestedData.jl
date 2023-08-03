module NestedIterators
using PooledArrays
using SumTypes
using Compat
using Accessors: @reset, @set
using ..NameLists: NameID, no_name_id
import ..get_name
import ..get_id
import ..make_switch
import ..opcompose

struct CaptureListNil end
"""A node in a linked list of captured iteration instructions"""
struct CaptureList{T}
    head::T
    tail::Union{CaptureListNil,CaptureList{T}}
end
CaptureList(seed) = CaptureList(seed, CaptureListNil())

Base.iterate(cl::CaptureList) = (cl.head, cl.tail)
Base.iterate(::CaptureList, state::CaptureList) = iterate(state)
Base.iterate(::CaptureList, ::CaptureListNil) = nothing

# IterCapture enumerates the kinds of instructions that can be captured and holds the values of those captures
@sum_type IterCapture :hidden begin
    RawSeed(::NameID)
    RawRepeat(::Int64)
    RawCycle(::Int64)
    RawVcat(::Vector{Int64}, ::Vector{CaptureList{<:IterCapture}})
end


struct RawNestedIterator
    get_index::Union{CaptureListNil,CaptureList}
    column_length::Int64
    el_type::Type
    one_value::Bool
    unique_val::NameID
end
"""
RawNestedIterator(csm, data; default_value=missing)

Construct a new RawNestedIterator seeded with the value data
# Args
csm::ColumnSetManager
data::Any: seed value
# Kwargs
default_value: Value to fill if data is empty
"""
function RawNestedIterator(csm, data::T; default_value=missing) where T
    value = if T <: AbstractArray
        length(data) == 0 ? (default_value,) : data
    else
        (data,)
    end
    is_one = allequal(value)
    len = length(value)
    val_T = typeof(value)
    id = get_id(csm, value)
    return RawNestedIterator(id, val_T, is_one, len)
end
function RawNestedIterator(value_id::NameID, ::Type{T}, is_one::Bool, len::Int64) where T
    E = eltype(T)
    f = CaptureList(IterCapture'.RawCycle(len), CaptureList(IterCapture'.RawSeed(value_id)))
    unique_val = is_one ? value_id : no_name_id
    return RawNestedIterator(f, len, E, is_one, unique_val)
end
RawNestedIterator() = RawNestedIterator(CaptureListNil(), 0, Union{}, false, no_name_id)

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
get_index_captures(rni::RawNestedIterator) = rni.get_index
is_single_value(rni::RawNestedIterator) = rni.one_value
get_unique_val(rni::RawNestedIterator) = rni.unique_val
get_el_type(rni::RawNestedIterator) = rni.el_type


"""Seed is the core starter for a NestedIterator"""
struct Seed{T}
    data::T
end
(s::Seed)(i) = s.data[i]
struct RawSeed
    data_id::NameID 
end
Seed(csm, raw_seed::RawSeed) = get_name(csm, raw_seed.data_id)

"""Captures the repeat value for a repeat_each call"""
struct UnrepeatEach 
    n::Int64
end
(u::UnrepeatEach)(i) = ceil(Int64, i/u.n)

function repeat_each(c::RawNestedIterator, n)
    @reset c.column_length = length(c) * n
    # when there is only one unique value, we can skip composing the repeat_each step
    c.one_value && return c

    @reset c.get_index = CaptureList(IterCapture'.RawRepeat(n), c.get_index)
    return c
end

"""Captures the repeat value for a cycle call"""
struct Uncycle
    n::Int64
end
(u::Uncycle)(i) = mod((i-1),u.n) + 1
"""cycle(c, n) cycles through an array N times"""
function cycle(c::RawNestedIterator, n)
    original_len = c.column_length
    @reset c.column_length = original_len * n
    
    # when there is only one unique value, we can skip composing the uncycle step
    c.one_value && return c

    new_step = IterCapture'.RawCycle(original_len)
    @reset c.get_index = CaptureList(new_step, c.get_index)
    return c
end

"""Captures the two getindex functions of vcated NestedIterators. f_len tells which index to break over to g."""
struct Unvcat{F}
    f::F
end
function Unvcat(csm, lengths, captures)
    funcs = (build_get_index(csm, cap) for cap in captures)
    f = make_switch(funcs, lengths)
    return Unvcat{typeof(f)}(f)
end
(u::Unvcat)(i) = u.f(i)

"""vcat(iters::RawNestedIterator...)
Return a single RawNestedIterator which is the result of stacking the iterators
"""
function Base.vcat(iters::RawNestedIterator...)
    lengths = length.(iters)
    len = sum(lengths)

    if all(is_single_value, iters) && allequal(Iterators.map(get_unique_val, iters))
        iter = first(iters)
        return @set iter.column_length = len
    end

    caps_list = collect(get_index_captures.(iters))
    type = Union{(getproperty(iter,:el_type) for iter in iters)...}

    RawNestedIterator(
        CaptureList(IterCapture'.RawVcat(collect(lengths), caps_list)), len, type, false, no_name_id)
end


"""NestedIterator is a finalized column with a custom function to reproduce the order 
that the data was found and unpacked"""
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
Base.collect(x::NestedIterator, pool_arrays=false) = Base.invokelatest(_collect, x, pool_arrays)
_collect(x, pool_arrays) = pool_arrays ? PooledArray(x) : Vector(x)

"""Compose a get_index function out of the list of captured instructions from a RawNestedIterator"""
function build_get_index(csm, captures)
    cap = captures.head
    new_f = get_iter_func(csm, cap)
    build_get_index(new_f, csm, captures.tail)
end
build_get_index(current_f, csm, cap) = build_get_index(current_f, csm, cap.head, cap.tail)
build_get_index(current_f, _, ::CaptureListNil) = current_f
function build_get_index(current_f, csm, cap, cap_tail)
    new_f = get_iter_func(csm, cap) ∘ current_f
    build_get_index(new_f, csm, cap_tail.head, cap_tail.tail)
end
build_get_index(current_f, csm, cap, ::CaptureListNil) = get_iter_func(csm, cap) ∘ current_f


"""Construct the correct iterator capture function for the given IterCapture"""
function get_iter_func(csm, capture::IterCapture)
    @cases capture begin
        RawSeed(id) => Seed(get_name(csm, id))
        RawRepeat(n) => UnrepeatEach(n)
        RawCycle(n) => Uncycle(n)
        RawVcat(lengths, captures_lists) => Unvcat(csm, lengths, captures_lists)
    end
end


end #NestedIterators
