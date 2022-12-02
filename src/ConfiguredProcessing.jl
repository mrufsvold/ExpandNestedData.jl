"""
    normalize(data, column_defs::Vector{ColumnDefinition})

Take a nested data structure, `data` and convert it into a `Table` based on configurations passed
for each column.

## Args
* `data`: Any nested data structure (struct of structs or Dict of Dicts) or an array of such data structures
* `column_defs::Vector{ColumnDefinition}`: A ColumnDefinition for each column to be extracted from the `data`

## Returns
`::NamedTuple`: A Tables.jl compliant Tuple of Vectors
"""
function normalize(data, column_defs::ColumnDefs)
    # TODO we should parse the user's column definitions into a graph before processing
    columns = process_node(data, column_defs)
    names = column_name.(column_defs)
    column_vecs = [
        collect(columns[field_path(def)], pool_arrays(def))
        for def in column_defs
    ]
    return NamedTuple{Tuple(names)}(column_vecs)
end


# Dispatch processing of an object to the correct version of process node using StructType
process_node(data::T, col_defs::ColumnDefs) where T = process_node(StructTypes.StructType(T), data, col_defs)


# Make a new column when you get to the bottom of the nested objects
function process_node(::D, data, col_defs::ColumnDefs) where D <: NameValueContainer
    (names, names_with_children) = analyze_column_defs(col_defs)
    columns = ColumnSet()
    data_names = get_names(data)
    multiplier = 1
    for name in names
        # This creates a view of configured columns to pass down
        child_col_defs = make_column_def_child_copies(col_defs, name)

        # Get child columns in 1 of 3 cases: 
        # name is in data and needs to be unpacked, is seed of a column, or name is missing 
        child_columns = if name in data_names
            child_data = get_value(data, name)
            child_columns = if name in names_with_children
                process_node(child_data, child_col_defs)
            else
                # If there are no children, there is only one column definition
                col_def = first(child_col_defs)
                new_column = NestedIterator(child_data; 
                    flatten_arrays = flatten_arrays(col_def), default_value=default_value(col_def))
                Dict([] => new_column)
            end
            prepend_name!(child_columns, name)
            child_columns
        else
            make_missing_column_set(child_col_defs, path_index(first(col_defs)))
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
function process_node(::A, data, col_defs::ColumnDefs) where A <: StructTypes.ArrayType
    # TODO Assert that all elements have name values pairs. 
    if length(data) == 0
        return make_missing_column_set(column_defs) 
    elseif length(data) == 1
        return process_node(first(data), column_defs)
    end

    all_column_sets = process_node.(data, Ref(col_defs))
    unique_names = all_column_sets .|> keys |> Iterators.flatten |> unique
    column_set = ColumnSet()
    for name in unique_names
        column_set[name] = all_column_sets .|>
            (col_set -> col_set[name]) |>
            (cols -> foldl(stack, cols))
    end
    return column_set
end









