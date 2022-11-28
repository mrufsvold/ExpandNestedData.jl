function normalize(data; expand_arrays::Bool = false, missing_value = missing, use_pool = false)
    columns = process_node(data; expand_arrays=expand_arrays, missing_value=missing_value)
    names = keys(columns)
    column_vecs = names .|> (n -> columns[n]) .|> (c -> collect(c, use_pool))
    return NamedTuple{Tuple(join_names(n) for n in names)}(column_vecs)
end


# Dispatch processing of an object to the correct version of process node using StructType
process_node(data::T; kwargs...) where T = process_node(StructTypes.StructType(T), data; kwargs...)


# Make a new column when you get to the bottom of the nested objects
process_node(::Any, data; kwargs...) = init_column_set(data, kwargs[:expand_arrays])


# If we get an array type, check if it should be expanded further or if it should be the seed of a new column
function process_node(data::AbstractArray{T}; kwargs...) where {T}
    # In the following cases, keep decending the tree
    continue_processing = (
        # If expand_arrays is true
        kwargs[:expand_arrays] ||
        # Empty array doesn't need further expansion
        length(data) == 0 ||
        # If all of the elements are name-value pair objects
        is_NameValueContainer(T) ||
        # Or if the elements are a union of types and any of them are name-value pair objects
        (T <: Union && has_namevaluecontainer_element(Base.uniontypes(T) )) || 
        # or if the elements are Any, we just need to check each one for name-value pair necessary
        (T == Any && has_namevaluecontainer_element(data))
    )
    if continue_processing
        return process_node(StructTypes.ArrayType(), data; kwargs...)
    end

    return process_node(nothing, data; kwargs...)
end


# handle unpacking arraylike objects
function process_node(::A, data; kwargs...) where A <: StructTypes.ArrayType
    if length(data) == 0
        return columnset(NestedIterator(kwargs[:missing_value]))
    elseif  length(data) == 1
        return process_node(first(data); kwargs...)
    end

    all_column_sets = process_node.(data; kwargs...)

    unique_names = all_column_sets .|> keys |> Iterators.flatten |> unique

    column_set = ColumnSet()
    for name in unique_names
        column_set[name] = all_column_sets         .|>
            (col_set -> get_column(col_set, name, kwargs[:missing_value]))  |>
            (cols -> foldl(stack, cols))
    end
    return column_set
end


# Handle a name-value pair object
function process_node(::D, data; kwargs...) where D <: NameValueContainer
    columns = ColumnSet()
    multiplier = 1
    for (child_name, child_data) in get_pairs(data)
        # Collect columns from the child's data
        child_columns = process_node(child_data; kwargs...)
        # Add the child's name to the key of all columns
        prepend_name!(child_columns, child_name)
        # Need to repeat each value for all of the values of the previous children
        # to make a product of values
        repeat_each!.(values(child_columns), multiplier)
        multiplier *= column_length(child_columns)
        merge!(columns, child_columns)
    end
    # catch up short columns with the total length for this group
    cycle_columns_to_length!(columns)
    return columns
end
