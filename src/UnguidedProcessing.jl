"""
    expand(data; flatten_arrays = false, default_value = missing, pool_arrays = false, column_names=Dict{Vector{Symbol}, Symbol}())

Take a nested data structure, `data` and convert it into a `Table`

## Args
* `data`: Any nested data structure (struct of structs or Dict of Dicts) or an array of such data structures

## Keyword Args
* `flatten_arrays`: When a leaf node is an array, should the values be flattened into separate rows or treated as a single value. Default: `true`
* `default_value`: When a certain key exists in one branch, but not another, what value should be used to fill missing. Default: `missing`
* `pool_arrays`: When collecting vectors for columns, choose whether to use `PooledArrays` instead of `Base.Vector`. Default: `false` (use `Vector`)
* `lazy_columns`: If true, return columns as a custom lazy iterator instead of collecting them as materialized vectors. Default: `false`
* `column_names::Dict{Vector{Symbol}, Symbol}`: Provide a mapping of key/fieldname paths to replaced column names
* `column_style`: Choose returned column style from `nested_columns` or `flat_columns`. If nested, `column_names` are ignored and a 
    TypedTables.Table is returned in which the columns are nested in the same structure as the source data. Default: `flat_columns`

## Returns
`::NamedTuple`: A Tables.jl compliant Tuple of Vectors
"""
function expand(data; flatten_arrays::Bool = false, default_value = missing, lazy_columns::Bool = false,
        pool_arrays::Bool = false, column_names::Dict{Vector{Symbol}, Symbol} = Dict{Vector{Symbol}, Symbol}(),
        column_style::ColumnStyle=flat_columns)
    columns = process_node(data; flatten_arrays=flatten_arrays, default_value=default_value)
    return ExpandedTable(columns, column_names; lazy_columns = lazy_columns, pool_arrays = pool_arrays, column_style = column_style)
end


# Dispatch processing of an object to the correct version of process node using StructType
process_node(data::T; kwargs...) where T = process_node(StructTypes.StructType(T), data; kwargs...)


# Make a new column when you get to the bottom of the nested objects
function process_node(::Any, data; kwargs...)
    value = data isa AbstractArray && length(data) == 0 && kwargs[:flatten_arrays] ?
        kwargs[:default_value] :
        data
    init_column_set(value, kwargs[:flatten_arrays])
end

# If we get an array type, check if it should be expanded further or if it should be the seed of a new column
function process_node(data::AbstractArray{T}; kwargs...) where {T}
    if length(data) > 0 && (kwargs[:flatten_arrays] || has_namevaluecontainer_element(data))
        return process_node(StructTypes.ArrayType(), data; kwargs...)
    end

    return process_node(nothing, data; kwargs...)
end


# handle unpacking array-like objects
function process_node(::A, data; kwargs...) where A <: StructTypes.ArrayType
    if length(data) == 0
        return columnset(NestedIterator(kwargs[:default_value]))
    elseif  length(data) == 1
        return process_node(first(data); kwargs...)
    end

    all_column_sets = process_node.(data; kwargs...)

    unique_names = all_column_sets .|> keys |> Iterators.flatten |> unique

    column_set = ColumnSet()
    for name in unique_names
        column_set[name] = all_column_sets         .|>
            (col_set -> get_column(col_set, name, kwargs[:default_value]))  |>
            (cols -> foldl(stack, cols))
    end
    return column_set
end


# Handle a name-value pair object (dict or struct)
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
