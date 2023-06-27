"""A dict-like set of columns. The keys are Int64 ids for actual names that are stored
in the ColumnSetManager"""
mutable struct ColumnSet
    cols::Vector{Pair{Int64, NestedIterator}}
    len::Int64
end
ColumnSet() = ColumnSet(Pair{Int64, NestedIterator}[], 0)
function ColumnSet(p::Pair...)
    cs = ColumnSet(Pair{Int64, NestedIterator}[p...], 0)
    sort_keys!(cs)
    reset_length!(cs)
    return cs
end

# Dict Interface
function Base.empty!(cs::ColumnSet)
    empty!(cs.cols)
    cs.len = 0
end

Base.haskey(cs::ColumnSet, k) = insorted((k,0), cs.cols; by=first)

function Base.setindex!(cs::ColumnSet, v, k)
    insert!(cs, (k=>v))
    cs.len = max(cs.len, length(v))
    return cs
end

function Base.getindex(cs::ColumnSet, k)
    i = searchsortedfirst(cs.cols, (k,0); by=first)
    p = cs.cols[i]
    if p[1] != k
        throw(KeyError(k))
    end
    return p[2]
end

function Base.pop!(cs::ColumnSet, k)
    i = searchsortedfirst(cs.cols, (k,0); by=first)
    p = popat!(cs.cols, i)
    if p[1] != k
        throw(KeyError(k))
    end
    return p
end

function Base.push!(cs::ColumnSet, p::Pair) 
    push!(cs.cols, p)
end

function Base.merge!(cs1::ColumnSet, cs2::ColumnSet)
    append!(cs1.cols, cs2.cols)
    sort_keys!(cs1)
    cs1.len = max(cs1.len, cs2.len)
    return cs1
end

Base.keys(cs::ColumnSet) = (first(p) for p in cs.cols)
Base.values(cs::ColumnSet) = (last(p) for p in cs.cols)
Base.length(cs::ColumnSet) = length(cs.cols)
Base.insert!(cs::ColumnSet, p::Pair) = insert!(cs.cols, searchsortedfirst(cs.cols, p; by=first), p)

sort_keys!(cs::ColumnSet) = sort!(cs.cols; by=first, alg=InsertionSort)
"""Get the length of the longest column. This is almost always the length of all columns in the set
except in the midst of merging multiple sets together"""
column_length(cols::ColumnSet) = cols.len

"""Check for the longest column in a set and update column length of set"""
function reset_length!(cs::ColumnSet)
    len = 0
    for v in values(cs)
        len = max(len, length(v))
    end
    set_length!(cs, len)
    return cs
end

"""Force column length of set"""
function set_length!(cs::ColumnSet, l)
    cs.len = l
end
