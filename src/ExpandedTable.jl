
using Tables
using TypedTables

@enum ColumnStyle flat_columns nested_columns

"""
The functionality we want here is:

t = ExpandedTable(columns, column_names)

t.a_c == [2, missing, 1, missing]
t.a.c == [2, missing, 1, missing]

eachrow(t, :flatten) |> first == (a_b = 1, a_c = 2, d= 4)
eachrow(t, :nested) |> first == (a = (b = 1, c = 2), d = 4)
names(t) ==(a_b, a_c, d)

"""
struct ExpandedTable
    col_lookup # Dict( column_name => path )
    columns # TypedTable, nested in the same pattern as src_data
end


"""Build a nested NamedTuple of TypedTables from the columns following the same nesting structure
as the source data"""
function make_column_tuple(columns, path_graph::AbstractPathNode, lazy_arrays::Bool)
    children_tuple = NamedTuple(
        name(child) => make_column_tuple(columns, child, lazy_arrays::Bool)
        for child in children(path_graph)
    )
    return Table(children_tuple)
end
function make_column_tuple(columns, path_graph::ValueNode, lazy_arrays::Bool)
    lazy_column = columns[field_path(path_graph)]
    return lazy_arrays ? lazy_column : collect(lazy_column, pool_arrays(path_graph))
end


"""Construct an ExpandedTable from the results of `expand`"""
function ExpandedTable(columns::ColumnSet, column_names::Dict, lazy_arrays, pool_arrays)
    paths = keys(columns)
    col_defs = ColumnDefinition.(paths, Ref(column_names); pool_arrays=pool_arrays)
    return ExpandedTable(columns, col_defs, lazy_arrays)
end
function ExpandedTable(columns::ColumnSet, column_defs::ColumnDefs, lazy_arrays::Bool)
    path_graph = make_path_graph(column_defs)
    column_tuple = make_column_tuple(columns, path_graph, lazy_arrays)
    col_lookup = Dict(
        column_name(def) => field_path(def)
        for def in column_defs
    )
    return ExpandedTable(col_lookup, column_tuple)
end


# Get Tables
as_nested_table(t::ExpandedTable) = t.columns
function as_flat_table(t::ExpandedTable)
    return NamedTuple(
        name => foldl(getproperty, path, init=t.columns)
        for (name, path) in pairs(t.col_lookup) 
    )
end
