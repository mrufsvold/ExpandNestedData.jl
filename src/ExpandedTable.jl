@enum ColumnStyle flat_columns nested_columns

get_column_style(s::Symbol) = (flat=flat_columns, nested=nested_columns)[s]

struct ExpandedTable
    col_lookup::Dict{Symbol, Tuple} # Name of column => path into nested data
    columns # TypedTable, nested in the same pattern as src_data
end

"""Construct an ExpandedTable from the results of `create_columns`"""
function ExpandedTable(columns::OrderedRobinDict{K, T}, path_graph; lazy_columns, kwargs...) where {K, T<: NestedIterator{<:Any}}
    column_tuple = make_column_tuple(columns, path_graph, lazy_columns)
    col_lookup = Dict(
        get_final_name(val_node) => get_field_path(val_node)
        for val_node in get_all_value_nodes(path_graph)
    )
    return ExpandedTable(col_lookup, column_tuple)
end

"""Build a nested NamedTuple of TypedTables from the columns following the same nesting structure
as the source data"""
function make_column_tuple(col_set, node::Node, lazy_columns::Bool)
    return @cases node begin
        [Path,Value](n) => make_column_tuple(col_set, n, lazy_columns)
        Simple => throw(ErrorException("there should be no simple nodes when building the column tuple"))
    end
end
function make_column_tuple(col_set, path_graph::AbstractPathNode, lazy_columns::Bool)
    kvs = []
    for child in get_children(path_graph)
        push!(kvs, Symbol(get_name(child)) => make_column_tuple(col_set, child, lazy_columns))
    end

    children_tuple = NamedTuple(kvs)
    return Table(children_tuple)
end
function make_column_tuple(col_set, path_graph::ValueNode, lazy_columns::Bool)
    lazy_column = col_set[get_field_path(path_graph)]
    value_column =  lazy_columns ? lazy_column : collect(lazy_column, get_pool_arrays(path_graph))
    return value_column
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
