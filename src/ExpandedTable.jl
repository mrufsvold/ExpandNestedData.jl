@enum ColumnStyle flat_columns nested_columns

get_column_style(s::Symbol) = (flat=flat_columns, nested=nested_columns)[s]

struct ExpandedTable
    col_lookup::Dict{Symbol, Tuple} # Name of column => path into nested data
    columns # TypedTable, nested in the same pattern as src_data
end

"""Construct an ExpandedTable from the results of `create_columns`"""
function ExpandedTable(columns::OrderedRobinDict{K, T}, path_graph, csm; lazy_columns, kwargs...) where {K, T<: NestedIterator{<:Any}}
    column_tuple = make_column_tuple(columns, path_graph, lazy_columns, csm)
    col_lookup = Dict(
        get_name(csm, get_final_name(val_node)) => reconstruct_field_path(csm, get_field_path(val_node))
        for val_node in get_all_value_nodes(path_graph)
    )
    return ExpandedTable(col_lookup, column_tuple)
end

"""Build a nested NamedTuple of TypedTables from the columns following the same nesting structure
as the source data"""
function make_column_tuple(col_set, node::Node, lazy_columns::Bool, csm)
    column_t = lazy_columns ? NestedIterator : Union{Vector, PooledArray}
    return make_column_tuple(col_set, node, column_t, csm)
end
function make_column_tuple(col_set, node::Node, column_t::Type{T}, csm) where T
    return @cases node begin
        Path(n,c) => new_level(col_set, n, c, column_t, csm)
        Value(n, _, fp_id, pool, _) => new_column(col_set, n, fp_id, pool, column_t, csm)
        Simple => throw(ErrorException("there should be no simple nodes when building the column tuple"))
    end
end
function new_level(col_set, name_id, child_nodes, column_t::Type{T}, csm) where T
    children_table = get_children_table(col_set, name_id, child_nodes, column_t, csm)
    if name_id == top_level_id
        return children_table
    end
    return get_name(csm, name_id) => children_table
end

function get_children_table(col_set, name_id, child_nodes, column_t::Type{T}, csm) where T
    keyval_pairs = Vector{Pair{Symbol, Union{Table,T}}}(undef, length(child_nodes))
    for (i, child) in enumerate(child_nodes)
        keyval_pairs[i] = make_column_tuple(col_set, child, column_t, csm)
    end
    return Table(NamedTuple(keyval_pairs))

end
function new_column(col_set, name_id, field_path_id, pool_arrays, ::Type{T}, csm) where T
    field_path = reconstruct_field_path(csm, field_path_id)
    lazy_column = col_set[field_path]
    value_column =  T <: NestedIterator ? lazy_column : collect(lazy_column, pool_arrays)
    return get_name(csm, name_id) => value_column
end

# Get Tables
as_nested_table(t::ExpandedTable) = t.columns
function as_flat_table(t::ExpandedTable)
    return NamedTuple(
        # foldl here is apply get property to t.columns (a nested Typed Table) and then traversing down
        # the path provided in column look up to find the column that matches the name
        name => foldl(getproperty, path, init=t.columns)
        for (name, path) in pairs(t.col_lookup) 
    )
end
