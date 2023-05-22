function expand(data, column_defs=nothing; default_value = missing, lazy_columns::Bool = false,
        pool_arrays::Bool = false, column_names::Dict{Vector{Symbol}, Symbol} = Dict{Vector{Symbol}, Symbol}(),
        column_style::ColumnStyle=flat_columns, name_join_pattern = "_")

    columns = process_node(data, column_defs; default_value=default_value)
    args = column_defs isa Nothing ? 
        (columns, column_names) :
        (columns, column_defs) 
    return ExpandedTable(args...; lazy_columns = lazy_columns, pool_arrays = pool_arrays, column_style = column_style, name_join_pattern=name_join_pattern)
end

# Dispatch processing of an object to the correct version of process node using StructType
function process_node(data::T, col_defs, depth=1; kwargs...) where T
    struct_type = StructTypes.StructType(T)
    if typeof(struct_type) <: Container
        return process_node(StructTypes.StructType(T), data, col_defs, depth; kwargs...)
    end
    return init_column_set(data, depth-1)
end

# Unpack a name-value pair object
function process_node(::DictOrStruct, data, col_defs::C, depth; kwargs...) where {DictOrStruct <: NameValueContainer, C}
    columns = ColumnSet()
    multiplier = 1
    col_defs_provided = !(C <: Nothing)
    data_names = get_names(data)

    (required_names, names_with_children) = if col_defs_provided
        analyze_column_defs(col_defs)
    else
        (data_names, data_names)
    end

    for name in required_names
        # This creates a copy of configured columns to pass down
        child_col_defs = col_defs_provided ? make_column_def_child_copies(col_defs, name) : nothing
        # both are always true when unguided
        should_have_child = name in names_with_children
        data_has_name = name in data_names
        child_data = get_value(data, name, missing)

        # CASE 1: Expect a child node and find one, unpack it (captures all unguided)
        child_columns = if should_have_child && data_has_name
            process_node(child_data, child_col_defs, depth+1; kwargs...)
        # CASE 2: Expected a child node, but don't find it 
        elseif should_have_child && !data_has_name
            make_missing_column_set(child_col_defs, path_index(first(col_defs)))
        # CASE 3: We don't expect a child node: wrap any value in a new column
        elseif !should_have_child
            col_def = first(child_col_defs)
            new_column = NestedIterator(get_value(data, name, default_value(col_def)); default_value=default_value(col_def))
            columnset(new_column, depth)
        end
        
        if length(child_columns) > 0
            # make_missing_column_set already has the full path, so skip prepend
            prepend_name!(child_columns, name, depth)
            
            # Need to repeat each value for all of the values of the previous children
            # to make a product of values
            match_len_child_cols = Dict(
                key => repeat_each(col, multiplier)
                for (key, col) in child_columns
            )
            multiplier *= column_length(match_len_child_cols)
            merge!(columns, match_len_child_cols)
        end
    end
    if length(columns) > 0
        # catch up short columns with the total length for this group
        cycle_columns_to_length!(columns)
    end
    return columns
end


# handle unpacking array-like objects
function process_node(::ArrayLike, data, col_defs::C, depth; kwargs...) where {ArrayLike <: StructTypes.ArrayType, C}
    # todo -- if the data is an array, we could check eltype and skip unpacking (just make a nested iter)

    if length(data) == 0
        # If we have column defs, but the array is empty, that means we need to make a missing column_set
        columns = !(C <: Nothing) ?
            make_missing_column_set(col_defs, (col_defs |> first |> path_index)) :
            columnset(NestedIterator(kwargs[:default_value]), depth-1)
        return columns
    elseif  length(data) == 1
        return process_node(first(data), col_defs, depth; kwargs...)
    end

    all_column_sets = process_node.(data, Ref(col_defs), depth; kwargs...)

    unique_names = all_column_sets .|> keys |> Iterators.flatten |> unique
    column_set = ColumnSet()
    for name in unique_names
        # For each unique column name, get that column for the results of processing each element
        # in this array, and then stack them all
        column_set[name] = all_column_sets         .|>
            (col_set -> get_column(col_set, name, kwargs[:default_value]))  |>
            (cols -> foldl(stack, cols))
    end
    return column_set
end


