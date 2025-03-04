module ExpandNestedData2
using Accessors: @set
using AutoHashEquals: @auto_hash_equals
using SumTypes: @sum_type, @cases
using StructTypes: StructTypes
using PooledArrays: PooledArray
using TypedTables: FlexTable
using Base.Iterators: repeated, flatmap
const ifilter = Iterators.filter
const imap = Iterators.map
const izip = Iterators.zip

NameValueContainer = Union{StructTypes.DictType, StructTypes.DataType}
Container = Union{NameValueContainer, StructTypes.ArrayType}

is_container(t) = typeof(StructTypes.StructType(t)) <: Container

@enum ColumnStyle flat_columns nested_columns
@enum PoolArrayOptions NEVER ALWAYS AUTO

struct CustomMissingValue end
const MISSING = CustomMissingValue()

macro get(dict, key, default)
    quote
        let v = get($(esc(dict)), $(esc(key)), $MISSING)
            if v === $MISSING
                $(esc(default))
            else
                v
            end
        end
    end
end

macro getproperty(obj, key, default)
    quote
        if hasproperty($obj, $key)
            getproperty($obj, $key)
        else
            $default
        end
    end
end


include("PathGraph2.jl")

if false
    IterCapture = nothing
    T = nothing
    IC = nothing
    Seed = nothing
    Repeat = nothing
    Cycle = nothing
    Concat = nothing
end


###### Name Path Types
@auto_hash_equals cache = true struct NamePart
    name
end
NamePart(;name=nothing) = NamePart(name)

@auto_hash_equals struct NamePath
    parts::Vector{NamePart}
end
NamePath() = NamePath(NamePart[])
NamePath(parts...) = NamePath([NamePart(part) for part in parts])

function append(np::NamePath, name)
    new_parts = copy(np.parts)
    push!(new_parts, NamePart(name))
    return NamePath(new_parts)
end
function Base.string(np::NamePath)
    return join((np.parts[i].name for i in 1:np.len), ".")
end
function Base.getindex(np::NamePath, i::Int)
    return np.parts[i].name
end
function Base.length(np::NamePath)
    return length(np.parts)
end
function Base.lastindex(np::NamePath)
    return length(np.parts)
end
function Base.firstindex(::NamePath)
    return 1
end

include("ColumnDefinitions2.jl")

"""
    get_unique_current_names(name_paths, level)
Get all unique names for the given depth level for a list of `NamePath`s
"""
get_unique_current_names(name_paths, level) = unique((current_path_name(name_path, level) for name_path in name_paths))

current_path_name(name_path::NamePath, level) = name_path.parts[level]

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

###### Column Types
struct Column
    name::NamePath
    data::IterCapture
end
Base.length(c::Column) = length(c.data)
Base.eltype(c::Column) = eltype(c.data)
function Base.collect(c::Column; pool_arrays)
    if pool_arrays == AUTO
        pool_up_to = length(c) ÷ 5
        seeds = get_all_seeds(c.data, pool_up_to)
        if !isnothing(seeds)
            return PooledArray(c.data)
        end
    elseif pool_arrays == ALWAYS
        return PooledArray(c.data)
    end
    return collect(c.data)
end
function get_name_path(c::Column)
    return c.name
end

function expand(data;
        default_value=missing,
        pool_arrays=false,
        name_join_pattern="_",
        column_style=:flat,
        lazy_columns = false,
        column_names = ()
    )
    col_set = _expand(data, NamePath(), default_value)

    if column_style == :flat
        column_name_lookup = Dict(
            NamePath(parts...) => replacement
            for (parts, replacement) in column_names
        )

        final_pairs = (
            get_flattened_name_column_pair(c, column_name_lookup;
                pool_arrays,
                name_join_pattern,
                lazy_columns
                )
            for c in col_set
        )

        # TODO make this a FlexTable before 2.0
        return (; final_pairs...)
    end

    name_paths = get_name_path.(col_set)
    path_graph = make_path_graph(name_paths)
    make_nested_table(col_set, path_graph)
end

function get_flattened_name_column_pair(column, column_name_lookup; pool_arrays, name_join_pattern, lazy_columns)
    name = @get(column_name_lookup, column.name, join_name_path(column.name, name_join_pattern))
    data = if lazy_columns
        column.data
    else
        collect(column; pool_arrays=pool_arrays)
    end
    return name => data
end

function join_name_path(np::NamePath, join_pattern)
    parts = imap(p -> string(p.name), np.parts)
    joined = join(parts, join_pattern)
    return Symbol(joined)
end

function _expand(@nospecialize(data), name_path, default_value)
    T = typeof(data)
    StructT = typeof(StructTypes.StructType(T))
    if StructT <: StructTypes.DictType
        return _expand_dict(data, name_path, default_value)
    elseif StructT <: StructTypes.DataType
        return _expand_data_type(data, name_path, default_value)
    elseif StructT <: StructTypes.ArrayType
        return _expand_array(data, name_path, default_value)
    else
        return _expand_leaf(data, name_path)
    end
end

function _expand_dict(@nospecialize(data), name_path::NamePath, default_value)
    return _expand_name_value_container(data, keys(data), getindex, name_path, default_value)
end

function _expand_data_type(@nospecialize(data), name_path::NamePath, default_value)
    return _expand_name_value_container(data, propertynames(data), getproperty, name_path, default_value)
end

function _expand_name_value_container(@nospecialize(data), @nospecialize(names), getter, name_path::NamePath, default_value)
    if length(names) == 0
        return Column[]
    end

    list_of_column_sets = Vector{Column}[
        _expand(getter(data, name), append(name_path, name), default_value)
        for name in names
    ]
    return merge_columns!(list_of_column_sets)
end

function merge_columns!(list_of_column_sets)
    if isempty(list_of_column_sets)
        return Column[]
    end
    column_set = reduce(merge_columns!, list_of_column_sets)
    multiplier = length(last(column_set))
    map(c -> cycle_column(c, multiplier ÷ length(c)), column_set)
end
function merge_columns!(column_set1, column_set2)
    if isempty(column_set1)
        return column_set2
    elseif isempty(column_set2)
        return column_set1
    end

    multiplier = length(last(column_set1))
    repeated_column_set = map(c -> cycle_column(c, multiplier), column_set2)
    append!(column_set1, repeated_column_set)
    return column_set1
end

function cycle_column(column, n)
    return Column(
        column.name,
        cycle(column.data, n)
        )
end

function Base.vcat(columns::Column...)
    allequal(c.name for c in columns) || throw(ArgumentError("columns must have the same name"))
    return Column(
        columns[1].name,
        concat((c.data for c in columns)...)
        )
end

function _expand_array(@nospecialize(data), name_path, default_value)
    element_count = length(data)

    if element_count == 0
        return Column[Column(name_path, seed(default_value))]
    elseif element_count == 1
        return _expand(only(data), name_path, default_value)
    end

    container_count = sum(is_container, data)

    # No containers at all
    if container_count == 0
        return Column[Column(name_path, seed_vector(data, default_value))]
    end

    containers = ifilter(is_container, data)
    expanded = imap(_expand, containers, repeated(name_path), repeated(default_value))
    no_empties = collect(ifilter(!isempty, expanded))
    all_names = Set(flatmap(c -> (x.name for x in c), no_empties))
    stacked_columns = Column[stack_columns(no_empties, name, default_value) for name in all_names]

    if container_count != element_count
        loose_values = filter(!is_container, data)
        loose_columns = Column[Column(name_path, seed_vector(loose_values, default_value))]
        return merge_columns!((loose_columns, stacked_columns))
    end
    return stacked_columns
end

function stack_columns(column_sets, name, default_value)
    data = concat(map(c -> get_column(c, name, default_value).data, column_sets))
    Column(name, data)
end



function get_column(column_set, name_path, default_value)
    len = length(column_set[1])
    i = findfirst(c -> c.name == name_path, column_set)
    if isnothing(i)
        return cycle_column(Column(name_path, seed(default_value)), len)
    end
    return column_set[i]
end

function _expand_leaf(@nospecialize(data), name_path::NamePath)
    return Column[Column(name_path, seed(data))]
end


function make_nested_table(column_set, path_graph::PathNode, name_path::NamePath=NamePath())
    return @cases path_graph begin
        [TopLevelNode, BranchNode] => table_from_children(column_set, path_graph, name_path)
        LeafNode(name, default_value, pool_arrays, _) => collect(
            get_column(column_set, name_path, default_value); pool_arrays=pool_arrays)
    end
end

function table_from_children(column_set, path_graph, name_path)
    children = get_children(path_graph)
    return FlexTable(;
        (
            Symbol(string(get_name(child))) =>make_nested_table(
                column_set, child, append(name_path, get_name(child))
            )
            for child in children
        )...
    )
end

end # END MODULE HERE #
