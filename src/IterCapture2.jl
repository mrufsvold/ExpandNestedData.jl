###### Iter Instruction Types
@sum_type IterCapture{T} <: AbstractVector{T} :hidden begin
    Seed{T}(data::T)
    SeedVector{T}(data::Vector{T})
    Repeat{T}(len::Int, child::IterCapture{T}, n::Int)
    Cycle{T}(len::Int, child::IterCapture{T})
    Concat(len::Int, children_n::Int, children::Vector{Pair{Int,IterCapture}})
end
function get_children(ic::IterCapture)
    @cases ic begin
        Concat(_, _, children) => map(last, children)
        [Repeat, Cycle](_, child,_) => (child,)
        [Seed, SeedVector] => nothing
    end
end

Base.eltype(::IterCapture{T}) where T = T

function Base.length(ic::IterCapture{T}) where T
    @cases ic begin
        Seed => 1
        SeedVector(data) => length(data)
        [Repeat, Cycle, Concat](len, _...) => len
    end
end

function get_all_seeds(ic::IterCapture, up_to::Int=64)
    seeds = @cases ic begin
        Seed(data) => Set((data,))
        SeedVector(data) => Set(data)
        [Repeat, Cycle,](_, child) => get_all_seeds(child)
        Concat(_, _, children) => union((get_all_seeds(last(child)) for child in children)...)
    end

    return if isnothing(seeds)
        nothing
    elseif up_to < length(seeds)
        nothing
    else
        seeds
    end
end


Base.size(ic::IterCapture) = (length(ic),)
seed(data::T) where T = IterCapture'.Seed{T}(data)
function seed_vector(@nospecialize(data), default_value)
    if length(data) == 0
        return seed(default_value)
    elseif length(data) == 1
        return seed(only(data))
    elseif data isa Vector
        return IterCapture'.SeedVector{eltype(data)}(data)
    end
    return IterCapture'.SeedVector{eltype(data)}(collect(data))
end

repeat(ic::IterCapture{T}, n::Int) where T= IterCapture'.Repeat{T}(n*length(ic), ic, n)
cycle(ic::IterCapture{T}, n::Int) where T = IterCapture'.Cycle{T}(n*length(ic), ic)
function concat(ics)
    T = Union{eltype.(ics)...}
    n = length(ics)
    final_indices = accumulate(+, length.(ics))
    children = Pair{Int,IterCapture}[
        i => ic
        for (i, ic) in izip(final_indices, ics)
    ]
    len = last(final_indices)
    res::IterCapture{T} = IterCapture'.Concat(len, n, children)
    return res
end
concat(ics::IterCapture...) = concat(ics)
function unconcat(i::Int, children::Vector{Pair{Int,IterCapture}}, ::Type{E}) where E
    child_index = searchsortedfirst(children, (i,); by=first)
    prev_last_index = child_index == 1 ? 0 : first(children[child_index-1])
    child = last(children[child_index])
    child[i-prev_last_index]
end
function Base.getindex(current_ic::IterCapture{T}, i::Int) where T
    length(current_ic) < i && throw(BoundsError(current_ic, i))
    return @cases current_ic begin
        Seed(data) => data::T
        SeedVector(data) => data[i]::T
        Repeat(len, ic, n) => ic[ceil(Int64, i/n)]::T
        Cycle(len, ic) => ic[mod((i-1), length(ic)) + 1]::T
        Concat(_, n, children) => unconcat(i, children, T)::T
    end
end
