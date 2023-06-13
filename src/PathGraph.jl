##### PathGraph #####
#####################


abstract type AbstractPathNode end

"""A node in the ColumnDefinition graph that has children"""
struct PathNode{T} <: AbstractPathNode
    name
    children::Vector{T}
end

"""A node in the ColumnDefinition graph that points to a leaf/value"""
struct ValueNode <: AbstractPathNode
    name
    final_name::Symbol
    field_path::Tuple
    pool_arrays::Bool
    default::NestedIterator
end

"""A node to capture a name (for emulating node behavior when unguided)"""
struct SimpleNode <: AbstractPathNode
    name
end

@sum_type Node :hidden begin
    Path(::PathNode)
    Value(::ValueNode)
    Simple(::SimpleNode)
end

wrap(n::PathNode) = Node'.Path(n)
wrap(n::ValueNode) = Node'.Value(n)
wrap(n::SimpleNode) = Node'.Simple(n)

get_name(w::Node) = @cases w begin [Path,Value,Simple](n) => get_name(n) end 

function ValueNode(name, field_path, pool_arrays, default; col_name)
    ValueNode(name, col_name, field_path, pool_arrays,default)
end


get_children(n::AbstractPathNode) = n.children
get_name(n::AbstractPathNode) = n.name
get_final_name(n::ValueNode) = n.final_name
get_field_path(n::ValueNode) = n.field_path
get_pool_arrays(n::ValueNode) = n.pool_arrays
get_default(n::ValueNode) = n.default

"""Given a certain level index, return the rest of the path down to the value"""
function path_to_value(c::ValueNode, current_index)
    fp = get_field_path(c)
    return fp[current_index:end]
end

function get_all_value_nodes(node::Node)
    value_node_channel = Channel{ValueNode}() do ch
        get_all_value_nodes(node, ch)
    end
    return collect(value_node_channel)
end
function get_all_value_nodes(node::Node, ch)
    @cases node begin
        Path(n) => get_all_value_nodes.(get_children(n), Ref(ch))
        Value(n) => put!(ch, n)
        Simple => throw(ErrorException("Cannot retrieve value nodes from a simple node"))
    end
    return nothing
end


function make_path_nodes!(column_defs, level = 1)
    unique_names = get_unique_current_names(column_defs, level)
    nodes = Vector{Node}(undef, length(unique_names))
    for (i, unique_name) in enumerate(unique_names)
        matching_defs = filter(p -> current_path_name(p, level) == unique_name, column_defs)
        are_value_nodes = [!has_more_keys(def, level) for def in matching_defs]
        
        all_value_nodes = all(are_value_nodes)
        mix_of_node_types = !all_value_nodes && any(are_value_nodes)

        if all_value_nodes
            # If we got to a value node, there should only be one.
            def = first(matching_defs)
            nodes[i] = wrap(ValueNode(
                unique_name, get_field_path(def), get_pool_arrays(def), NestedIterator(get_default_value(def));
                col_name = get_column_name(def)))
            continue
        end

        with_children = !mix_of_node_types ? 
            matching_defs :
            [def for (is_value, def) in zip(are_value_nodes, matching_defs) if !is_value]
        children_column_defs = make_column_def_child_copies(with_children, unique_name, level)

        child_nodes = make_path_nodes!(children_column_defs, level+1)
        if mix_of_node_types
            without_child_idx = findfirst(identity, are_value_nodes)
            without_child = matching_defs[without_child_idx]
            value_column_node = ValueNode(
                unnamed(), 
                (get_field_path(without_child)..., unnamed()), 
                get_pool_arrays(without_child),
                NestedIterator(get_default_value(without_child));
                col_name=get_column_name(without_child))
            push!(child_nodes, wrap(value_column_node))
        end

        nodes[i] = wrap(PathNode(unique_name, child_nodes))
    end
    return nodes
end 


"""Create a graph of field_paths that models the structure of the nested data"""
make_path_graph(column_defs) = wrap(PathNode(:TOP_LEVEL, make_path_nodes!(column_defs)))
make_path_graph(::Nothing; _...) = wrap(SimpleNode(nothing))
