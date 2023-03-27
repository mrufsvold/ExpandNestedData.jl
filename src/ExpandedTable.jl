
using Tables
using TypedTables

@enum ColumnStyle flat_columns nested_columns


struct ExpandedTable
    col_lookup::Dict{Symbol, Vector} # Name of column => path into nested data
    columns # TypedTable, nested in the same pattern as src_data
end


"""Build a nested NamedTuple of TypedTables from the columns following the same nesting structure
as the source data"""
function make_column_tuple(columns, path_graph::AbstractPathNode, lazy_columns::Bool)
    children_tuple = NamedTuple(
        Symbol(name(child)) => make_column_tuple(columns, child, lazy_columns::Bool)
        for child in children(path_graph)
    )
    return Table(children_tuple)
end
function make_column_tuple(columns, path_graph::ValueNode, lazy_columns::Bool)
    lazy_column = columns[field_path(path_graph)]
    return lazy_columns ? lazy_column : collect(lazy_column, pool_arrays(path_graph))
end


"""Construct an ExpandedTable from the results of `expand`"""
function ExpandedTable(columns::Dict{Vector, T}, col_defs; lazy_columns=false, pool_arrays=false, column_style=flat_columns, name_join_pattern = "_") where {T<: NestedIterator{<:Any}}
    sym_key_columns = Dict(
        Symbol.(k) => v 
        for (k, v) in pairs(columns)
    )
    return ExpandedTable(sym_key_columns, col_defs; lazy_columns =lazy_columns, pool_arrays=pool_arrays, column_style = column_style, name_join_pattern)
end
function ExpandedTable(columns::Dict{Vector{Symbol}, T}, column_names::Dict; lazy_columns=false, pool_arrays=false, column_style=flat_columns, name_join_pattern = "_") where {T<: NestedIterator{<:Any}}
    paths = keys(columns)
    col_defs = ColumnDefinition.(paths, Ref(column_names); pool_arrays=pool_arrays, name_join_pattern)
    return ExpandedTable(columns, col_defs; lazy_columns =lazy_columns, column_style = column_style)
end
function ExpandedTable(columns::Dict{Vector{Symbol}, T}, column_defs::ColumnDefs; kwargs...) where {T<: NestedIterator{<:Any}}
    path_graph = make_path_graph(column_defs)
    column_tuple = make_column_tuple(columns, path_graph, kwargs[:lazy_columns])
    col_lookup = Dict(
        column_name(def) => field_path(def)
        for def in column_defs
    )
    expanded_table = ExpandedTable(col_lookup, column_tuple)
    
    column_style = kwargs[:column_style]
    if column_style == flat_columns
        return as_flat_table(expanded_table)
    elseif column_style == nested_columns
        return as_nested_table(expanded_table)
    end
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
