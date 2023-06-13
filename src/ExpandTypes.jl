"""NameValueContainer is an abstraction on Dict and DataType structs so that we can get their
contents without worrying about `getkey` or `getproperty`, etc.
"""
NameValueContainer = Union{StructTypes.DictType, StructTypes.DataType}
Container = Union{StructTypes.DictType, StructTypes.DataType, StructTypes.ArrayType}

is_NameValueContainer(t) = typeof(StructTypes.StructType(t)) <: NameValueContainer
is_container(t) = typeof(StructTypes.StructType(t)) <: Container
is_value_type(t::Type) = !is_container(t) && isconcretetype(t)


@sum_type NameList :hidden begin
    Empty(::Nil{Int64})
    Head(::Cons{Int64})
end
function NameList(i::Int64, tail::NameList)
    return @cases tail begin
        [Empty,Head](t) => NameList'.Head(cons(i, t))
    end
end

@sum_type UnpackStep :hidden begin
                #name, data, path_node
    dict(::NameList, ::Any, ::Node)
    arr(::NameList, ::AbstractArray, ::Node)
    leaf(::NameList, ::Any)
    default(::NameList)
                #num_columns
    merge_cols(::Int64)
    stack_cols(::Int64)
    columns(::ColumnSet)
end

leaf_step(n,d,::Any) = UnpackStep'.leaf(n,d)

struct ExpandMissing end

function get_name(u::UnpackStep)
    name = @cases u begin
        [dict,arr](n,d,p) => n
        [leaf,default,merge_cols,stack_cols](n,d) => n 
        columns => throw(ErrorException("columns step has no name"))
    end
    return name
end
function get_data(u::UnpackStep)
    data = @cases u begin
        [dict,arr](n,d,p) => d
        [leaf,default,merge_cols,stack_cols](n,d) => d
        columns(c) => c
    end
    return data
end
function get_path_node(u::UnpackStep)
    node = @cases u begin
        [dict,arr](n,d,p) => p
        [leaf,default,merge_cols,stack_cols, columns] => throw(ErrorException("step does not contain a path node"))
    end
    node
end

##### ColumnDefinition #####
############################

"""ColumnDefinition provides a mechanism for specifying details for extracting data from a nested data source"""
struct ColumnDefinition
    # Path to values
    field_path::Tuple
    # name of this column in the table once expanded
    column_name::Symbol
    default_value
    pool_arrays::Bool
end
# Accessors
get_field_path(c::ColumnDefinition) = c.field_path
get_column_name(c::ColumnDefinition) = c.column_name
get_default_value(c::ColumnDefinition) = c.default_value
get_pool_arrays(c::ColumnDefinition) = c.pool_arrays

"""
    ColumnDefinition(field_path; column_name=nothing, flatten_arrays=false, default_value=missing, pool_arrays=false)

## Args
* `field_path`: Vector or Tuple of keys/fieldnames that constitute a path from the top of the data to the values to extract for the column

## Keyword Args
* `column_name::Symbol`: A name for the resulting column. If `nothing`, defaults to joining the `field_path` with snake case format.
* `flatten_arrays::Bool`: When a leaf node is an array, should the values be flattened into separate rows or treated as a single value. Default: `true`
* `default_value`: When the field_path keys do not exist on one or more branches, fill with this value. Default: `missing`
* `pool_arrays::Bool`: When collecting values for this column, choose whether to use `PooledArrays` instead of `Base.Vector`. Default: `false` (use `Vector`)
* `name_join_pattern::String`: The separator for joining field paths into column names. Default: "_"
## Returns
`::ColumnDefinition`
"""
function ColumnDefinition(field_path; kwargs...)
    return ColumnDefinition(tuple(field_path...); kwargs...)
end
function ColumnDefinition(field_path::T; column_name=nothing, default_value=missing, pool_arrays=false, name_join_pattern::String = "_") where {T <: Tuple}
    if column_name isa Nothing
        path = last(field_path) == unnamed() ? field_path[1:end-1] : field_path
        column_name = join_names(path, name_join_pattern)
    end
    ColumnDefinition(field_path, column_name, default_value, pool_arrays)
end
function ColumnDefinition(field_path, column_names::Dict; pool_arrays::Bool, name_join_pattern = "_")
    column_name = haskey(column_names, field_path) ? column_names[field_path] : nothing
    ColumnDefinition(field_path; column_name=column_name, pool_arrays=pool_arrays, name_join_pattern = name_join_pattern)
end
function construct_column_definitions(columns, column_names, pool_arrays, name_join_pattern)
    paths = keys(columns)
    return ColumnDefinition.(paths, Ref(column_names); pool_arrays=pool_arrays, name_join_pattern)
end

function current_path_name(c::ColumnDefinition, level)
    fp = get_field_path(c)
    return fp[level]
end

"""
    get_unique_current_names(defs, level)
Get all unique names for the given depth level for a list of ColumnDefinitions
"""
get_unique_current_names(defs, level) = unique((current_path_name(def, level) for def in defs))

"""
    make_column_def_child_copies(column_defs::Vector{ColumnDefinition}, name, level)
Return a column definitions that have children for the given name at the given level.
"""
function make_column_def_child_copies(column_defs::Vector{ColumnDefinition}, name, level)
    mask = findall(
        def -> is_current_name(def, name, level) && length(get_field_path(def)) > level, 
        column_defs
        )
    return view(column_defs, mask)
end
"""
    is_current_name(column_def::ColumnDefinition, name, level)
Check if name matches the field path for column_def at level
"""
is_current_name(column_def::ColumnDefinition, name, level) = current_path_name(column_def, level) == name
"""
    has_more_keys(column_def, level)
Check if there are more keys in the field path below the given level
"""
has_more_keys(column_def, level) = level < length(get_field_path(column_def))
