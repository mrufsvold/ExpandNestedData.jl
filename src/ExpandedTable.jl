using Tables
using TypedTables

@enum ColumnStyle flat_columns nested_columns

get_column_style(s::Symbol) = (flat=flat_columns, nested=nested_columns)[s]

struct ExpandedTable
    col_lookup::Dict{Symbol, Tuple} # Name of column => path into nested data
    columns # TypedTable, nested in the same pattern as src_data
end

"""Construct an ExpandedTable from the results of `create_columns`"""
function ExpandedTable(columns::Dict{K, T}, column_defs::Vector{ColumnDefinition}; lazy_columns, column_style, kwargs...) where {K, T<: NestedIterator{<:Any}}
    path_graph = make_path_graph(column_defs)
    column_tuple = make_column_tuple(columns, path_graph, lazy_columns)
    col_lookup = Dict(
        column_name(def) => field_path(def)
        for def in column_defs
    )
    expanded_table = ExpandedTable(col_lookup, column_tuple)
    
    if column_style == flat_columns
        return as_flat_table(expanded_table)
    elseif column_style == nested_columns
        return as_nested_table(expanded_table)
    end
end

"""Build a nested NamedTuple of TypedTables from the columns following the same nesting structure
as the source data"""
function make_column_tuple(columns, path_graph::AbstractPathNode, lazy_columns::Bool)
    kvs = []
    for child in children(path_graph)
        push!(kvs, Symbol(get_name(child)) => make_column_tuple(columns, child, lazy_columns))
    end

    children_tuple = NamedTuple(kvs)
    return Table(children_tuple)
end
function make_column_tuple(columns, path_graph::ValueNode, lazy_columns::Bool)
    lazy_column = columns[field_path(path_graph)]
    value_column =  lazy_columns ? lazy_column : collect(lazy_column, pool_arrays(path_graph))
    if length(children(path_graph)) > 0
        d = Dict(:unnamed => value_column)
        for child in children(path_graph)
            d[Symbol(get_name(child))] = make_column_tuple(columns, child, lazy_columns)
        end
        return Table(NamedTuple(d))
    end
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
