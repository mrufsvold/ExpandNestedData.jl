abstract type AbstractColumnDefinition end
struct ColumnDefinition{T} <: AbstractColumnDefinition where {T <: AbstractArray}
    field_path
    column_name::Union{Symbol, Nothing}
    expand_array::Bool
    value_if_missing
    vector_type::T
end

function ColumnDefinition(field_path; column_name=nothing, expand_array=false, value_if_missing=missing, vector_type=Vector)
    column_name = column_name isa Nothing ? join_path_names(field_path) : column_name
    ColumnDefinition(field_path, column_name, expand_array, value_if_missing, vector_type)
end
# Accessors
field_path(c) = c.field_path
get_column_name(c) = c.column_name
default_value(c) = c.value_if_missing
vector_type(c) = c.vector_type
expand_array(c) = c.expand_array


function make_column_def_child_copies(column_defs, name)
    return column_defs |>
    pfilter(first_path_name_is(name)) |>
    pfilter(col -> more_than_one_el(field_path(col))) .|>
    # make a NamedTuple with matching property names to pass down
    (col -> ColumnDefinition(
        (@view field_path(col)[2:end]), 
        get_column_name(col),
        col.expand_array,
        col.value_if_missing,
        col.vector_type
        ))
end


# Versions of make_path_graph that use ColumnDefinition
function make_path_graph(data, column_defs::Vector{T}) where T <: AbstractColumnDefinition
    make_path_graph(data, column_defs, 1)
end
function make_path_graph(data::D, column_defs::Vector{T}, left_siblings_product) where {D <: AbstractDict, T <: AbstractColumnDefinition}
    (names, has_children) = find_unique_keys(column_defs)
    records = 1
    generators = Dict{Symbol, ColumnGenerator}()
    for name in names
        if name in has_children
            # This creates a view of configured columns to pass down
            child_columns = make_column_def_child_copies(column_defs, name)
            # TODO need to handle what to do if a who limb of the graph is missing
            child_node = make_path_graph(data[name], child_columns, records)

            # We need to cycle through each generator as many times as the product of all the 
            # sibling we've found so far. This makes the first sibling cycle fastest and each 
            # successive sibling repeat each element once for each cycle of its sibling to the "left"
            for (column_name, generator) in pairs(child_node.generators)
                generators[column_name] = repeat_generator(generator, left_siblings_product)
            end
            records *= child_node.length
        else
            # If we don't have any children, we need to make leaf nodes
            index = findfirst(first_path_name_is(name), column_defs)
            column_def = column_defs[index]
            column_name = get_column_name(column_def)
            expand = expand_array(column_def)
            default = default_value(column_def)

            # This ifelse handles if a key is missing
            child_node = if name in keys(data)
                make_path_graph(data[name], expand, records, false, default)
            else
                make_path_graph(default, false, records, true)
            end
            generators[column_name] = repeat_generator(child_node, left_siblings_product)
            records *= child_node.length
        end
    end
    
    return (length = records, generators = generators)
end
function make_path_graph(data::A, column_defs::Vector{D}, left_siblings_product) where {A <: AbstractArray, D <: AbstractColumnDefinition}
    # When we get to an array, we need to get all the generators from the element objects
    # of that array and stack them
    children = (make_path_graph(child_data, column_defs, 1) for child_data in data)
    records = children .|> (c -> getproperty(c, :length)) |> sum
    generators = Dict{Symbol, ColumnGenerator}()
    for column_name in get_column_name.(column_defs)
        all_child_records = flatten_generators(children, column_name)
        generators[column_name] = repeat_generator(all_child_records, left_siblings_product)
    end
    return (length=records, generators=generators)
end


"""
normalize(data, column_defs)

Extract records from a JSON object and return them as a table.

Example:
```julia
json_data = \"""
{
    "a" : [
        {"b" : 1, "c" : 2},
        {"b" : 2},
        {"b" : [3, 4], "c" : 1}
    ],
    "d" : 4
}\"""
columns = [
        JSONTables.ColumnDefinition([:a, :b]; expand_array=true),
        JSONTables.ColumnDefinition([:a, :c]; value_if_missing="Oh, no!"),
        JSONTables.ColumnDefinition([:d]; column_name = :col3)
        ]
normalize(json_data, columns)
#(a_b=[1,2,3,4], a_c=[2,"Oh, no!",1,1], col3=[4,4,4,4])
```

Args:
    data - A string or JSON3.read() result containing a JSON object
    column_defs::Vector{ColumnDefinition} - A list of column definitions to extract
Kwargs:
    sink - A Table.jl target to write values to
Returns:
    ::NamedTuple or type of sink
"""
function normalize(data::T, column_defs::Vector{C}; sink=nothing) where {T <: Union{AbstractArray, AbstractDict}, C <: AbstractColumnDefinition}
    graph = make_path_graph(data, column_defs)

    table = NamedTuple(
        get_column_name(c) => collect_column_generator(
                                                graph.generators[get_column_name(c)], 
                                                graph.length, 
                                                vector_type(c)) 
        for c in column_defs
    )
    if (sink isa Nothing)
        return table
    else
        return table |> sink
    end
end 
