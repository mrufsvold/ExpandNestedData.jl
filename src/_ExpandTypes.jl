@enum StepType dict arr leaf default merge_cols stack_cols columns

struct ExpandMissing end
struct UnpackStep{N,T,C}
    type::StepType
    name::N
    data::T
    level::Int64
    path_node::C
end
get_step_type(u::UnpackStep) = u.type
get_name(u::UnpackStep) = u.name
get_data(u::UnpackStep) = u.data
get_level(u::UnpackStep) = u.level
get_path_node(u::UnpackStep) = u.path_node

"""NameValueContainer is an abstraction on Dict and DataType structs so that we can get their
contents without worrying about `getkey` or `getproperty`, etc.
"""
NameValueContainer = Union{StructTypes.DictType, StructTypes.DataType}
Container = Union{StructTypes.DictType, StructTypes.DataType, StructTypes.ArrayType}

is_NameValueContainer(t) = typeof(StructTypes.StructType(t)) <: NameValueContainer
is_container(t) = typeof(StructTypes.StructType(t)) <: Container
is_value_type(t::Type) = !is_container(t) && isconcretetype(t)

##### ColumnDefinition #####
############################

"""ColumnDefinition provides a mechanism for specifying details for extracting data from a nested data source"""
mutable struct ColumnDefinition
    # Path to values
    field_path::Tuple
    # name of this column in the table once expanded
    const column_name::Symbol
    const default_value
    const pool_arrays::Bool
end
# Accessors
field_path(c::ColumnDefinition) = c.field_path
column_name(c::ColumnDefinition) = c.column_name
default_value(c::ColumnDefinition) = c.default_value
pool_arrays(c::ColumnDefinition) = c.pool_arrays

"""
    ColumnDefinition(field_path; column_name=nothing, flatten_arrays=false, default_value=missing, pool_arrays=false)

## Args
* `field_path`: Vector of keys/fieldnames that constitute a path from the top of the data to the values to extract for the column

## Keyword Args
* `column_name::Symbol`: A name for the resulting column. If `nothing`, defaults to joining the field_path with snake_case_format.
* `flatten_arrays::Bool`: When a leaf node is an array, should the values be flattened into separate rows or treated as a single value. Default: `true`
* `default_value`: When the field_path keys do not exist on one or more branches, fill with this value. Default: `missing`
* `pool_arrays::Bool`: When collecting values for this column, choose whether to use `PooledArrays` instead of `Base.Vector`. Default: `false` (use `Vector`)
* `name_join_pattern::String`: The separator for joining field paths into column names. Default: "_"
## Returns
`::ColumnDefinition`
"""
function ColumnDefinition(field_path; column_name=nothing, default_value=missing, pool_arrays=false, name_join_pattern::String = "_")
    if column_name isa Nothing
        path = last(field_path) == :unnamed ? field_path[1:end-1] : field_path
        column_name = join_names(path, name_join_pattern)
    end
    ColumnDefinition(field_path, column_name, default_value, pool_arrays)
end
function ColumnDefinition(field_path, column_names::Dict; pool_arrays::Bool, name_join_pattern = "_")
    column_name = field_path in keys(column_names) ? column_names[field_path] : nothing
    ColumnDefinition(field_path; column_name=column_name, pool_arrays=pool_arrays, name_join_pattern = name_join_pattern)
end

function current_path_name(c::ColumnDefinition, depth)
    fp = field_path(c)
    return fp[depth]
end
function path_to_children(c::ColumnDefinition, current_index)
    fp = field_path(c)
    return fp[current_index:end]
end
get_unique_current_names(defs, depth) = unique((current_path_name(def, depth) for def in defs))
function make_column_def_child_copies(column_defs::Vector{ColumnDefinition}, name, depth)
    return filter(
        def -> is_current_name(def, name, depth) && length(field_path(def)) > depth, 
        column_defs
        )
end
is_current_name(col_def::ColumnDefinition, name, depth) = current_path_name(col_def, depth) == name
has_more_keys(col_def, depth) = depth < length(field_path(col_def))
function append_name!(def, name)
    new_field_path = tuple(field_path(def)..., name)
    def.field_path = new_field_path
    return def
end
