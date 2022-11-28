
struct ColumnDefinition
    field_path
    path_index::Int64
    column_name::Symbol
    expand_array::Bool
    default_value
    use_pool::Bool
end
# Convenience alias
ColumnDefs = Vector{ColumnDefinition}
function ColumnDefinition(field_path; column_name=nothing, expand_array=false, default_value=missing, use_pool=false)
    column_name = column_name isa Nothing ? join_names(field_path) : column_name
    ColumnDefinition(field_path, 1, column_name, expand_array, default_value, use_pool)
end

# Accessors
field_path(c::ColumnDefinition) = c.field_path
column_name(c::ColumnDefinition) = c.column_name
default_value(c::ColumnDefinition) = c.default_value
use_pool(c::ColumnDefinition) = c.use_pool
expand_array(c::ColumnDefinition) = c.expand_array
path_index(c::ColumnDefinition) = c.path_index
function current_path_name(c::ColumnDefinition)
    fp = field_path(c)
    i = current_path_index(c)
    return fp[i]
end


is_current_name(col_def::ColumnDefinition, name) = current_path_name(col_def) == name

has_more_keys(col_def) = current_path_index(col_def) < length(field_path(col_def))


function analyze_column_defs(col_defs::ColumnDefs)
    unique_names = col_defs .|> current_path_name |> unique
    names_with_children = filter(has_more_keys, col_defs) .|> current_path_name |> unique
    return (unique_names, names_with_children)
end

function make_column_def_child_copies(column_defs::ColumnDefs, name)
    return filter((def -> is_current_name(def, name)), column_defs) .|>
        (def -> ColumnDefinition(
            field_path(def),
            current_path_index(def),
            column_name(def),
            expand_array(def),
            default_value(def),
            vector_type(def)
        ))
end


# Dispatch processing of an object to the correct version of process node using StructType
process_node(data::T, col_defs::ColumnDefs; kwargs...) where T = process_node(StructTypes.StructType(T), data; kwargs...)


# Make a new column when you get to the bottom of the nested objects
function process_node(::D, data, col_defs::ColumnDefs; kwargs...) where D <: NameValueContainer
    (names, names_with_children) = analyze_column_defs(col_defs)
    columns = ColumnSet()
    data_names = get_names(data)
    multiplier = 1
    for name in names
        # This creates a view of configured columns to pass down
        child_col_defs = make_column_def_child_copies(column_defs, name)

        # Get child columns in 1 of 3 cases: name is in data and needs to be unpacked, is seed of a column, or name is missing 
        child_columns = if name in data_names
            child_data = get_value(data, name)
            if name in names_with_children
                process_node(child_data, child_col_defs; kwargs...)
            else
                # If there are no children, there is only one column definition
                col_def = first(child_col_defs)
                new_column = NestedIterator(child_data; expand_arrays = expand_array(col_def))
                Dict(column_name(col_def) => new_column)
            end
        else
            make_missing_column_set(child_col_defs)
        end

        repeat_each!.(values(child_columns), multiplier)
        multiplier *= column_length(child_columns)
        merge!(columns, child_columns)
    end
    # catch up short columns with the total length for this group
    cycle_columns_to_length!(columns)
    
    return columns
end

# handle unpacking arraylike objects
function process_node(::A, data, col_defs::ColumnDefs; kwargs...) where A <: StructTypes.ArrayType
    # TODO Assert that all elements have name values pairs. 
    if length(data) == 0
        return make_missing_column_set(child_col_defs) 
    elseif length(data) == 1
        return process_node(first(data), column_defs; kwargs...)
    end

    all_column_sets = process_node.(data, col_defs; kwargs...)

    column_set = ColumnSet()
    for def in col_defs
        name = column_name(def)
        column_set[name] = all_column_sets .|>
            (col_set -> col_set[name]) |>
            (cols -> foldl(stack, cols))
    end
end


function make_missing_column_set(child_col_defs::ColumnDefs)
    missing_column_set =  Dict(
        column_name(def) => NestedIterator(default_value(def))
        for def in child_col_defs
    )
    return missing_column_set
end







