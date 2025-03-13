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

include("Utils2.jl")
include("IterCapture2.jl")
include("NamePath2.jl")
include("ColumnDefinitions2.jl")
include("PathGraph2.jl")


function expand(data;
        default_value=missing,
        pool_arrays=false,
        name_join_pattern="_",
        column_style=:flat,
        lazy_columns = false,
        column_names = ()
    )
    col_set = _expand(data, NamePath(), default_value)

    pool_arrays = pool_arrays ? AUTO : NEVER

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
        LeafNode(name, column_definition) => collect(
            get_column(
                column_set,
                name_path,
                column_definition.
                default_value);
            pool_arrays=column_definition.pool_arrays
            )
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
