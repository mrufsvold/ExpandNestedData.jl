module PathGraph
using SumTypes
using ..ColumnSetManagers: ColumnSetManager, NameID, get_id, unnamed_id, unnamed, top_level_id, get_id_for_path
using ..NestedIterators: NestedIterator
using ..ColumnDefinitions
using ..ColumnDefinitions:  ColumnDefinition, 
                            get_unique_current_names, 
                            get_field_path, 
                            get_pool_arrays, 
                            get_default_value, 
                            get_column_name,
                            has_more_keys,
                            current_path_name,
                            make_column_def_child_copies
import ..get_name

export Node, SimpleNode, ValueNode, PathNode, get_name, get_children, get_all_value_nodes, get_default, make_path_graph, get_final_name

@sum_type Node :hidden begin
    Path(::NameID, ::Vector{Node})
    Value(::NameID, ::NameID, ::NameID, ::Bool, ::Ref{NestedIterator{<:Any, <:Any}})
    Simple(::NameID)
end

PathNode(csm::ColumnSetManager, name, children::Vector{Node}) = PathNode(get_id(csm, name), children)
PathNode(name::NameID, children::Vector{Node}) = Node'.Path(name, children)

function ValueNode(csm::ColumnSetManager, name, final_name, field_path, pool_arrays::Bool, default::NestedIterator)
    ValueNode(get_id(csm, name),  get_id(csm, final_name),  get_id_for_path(csm, field_path), pool_arrays, default)
end
ValueNode(name::NameID, final_name::NameID, field_path::NameID, pool_arrays::Bool, default::NestedIterator) = Node'.Value(name, final_name, field_path, pool_arrays, Ref{NestedIterator{<:Any, <:Any}}(default))

SimpleNode(csm::ColumnSetManager, name) = SimpleNode(get_id(csm, name))
SimpleNode(name::NameID) = Node'.Simple(name)

function get_name(node::Node)
    return @cases node begin 
        Path(n,_) => n
        Value(n,_,_,_,_) => n
        Simple(n) => n
    end
end
function get_children(node::Node)
    return @cases node begin 
        Path(_,c) => c
        [Value,Simple] => throw(ErrorException("Value and Simple nodes do not have children"))
    end
end
function get_final_name(node::Node)
    return @cases node begin 
        [Path, Simple] => throw(ErrorException("Path and Simple nodes do not have a final_name"))
        Value(_,n,_,_,_) => n
    end
end
function ColumnDefinitions.get_field_path(node::Node)
    return @cases node begin 
        [Path, Simple] => throw(ErrorException("Path and Simple nodes do not have a field_path"))
        Value(_,_,p,_,_) => p
    end
end
function ColumnDefinitions.get_pool_arrays(node::Node)
    return @cases node begin 
        [Path, Simple] => throw(ErrorException("Path and Simple nodes do not have a pool_arrays"))
        Value(_,_,_,p,_) => p
    end
end
function get_default(node::Node)
    return @cases node begin 
        [Path, Simple] => throw(ErrorException("Path and Simple nodes do not have a default"))
        Value(_,_,_,_,d) => d[]
    end
end


"""Given a certain level index, return the rest of the path down to the value"""
function path_to_value(c::Node, current_index)
    fp = get_field_path(c)
    return fp[current_index:end]
end

function get_all_value_nodes(node::Node)
    value_node_channel = Channel{Node}() do ch
        get_all_value_nodes(node, ch)
    end
    return collect(value_node_channel)
end
function get_all_value_nodes(node::Node, ch)
    @cases node begin
        Path => get_all_value_nodes.(get_children(node), Ref(ch))
        Value => put!(ch, node)
        Simple => throw(ErrorException("Cannot retrieve value nodes from a simple node"))
    end
    return nothing
end


function make_path_nodes!(csm, column_defs::AbstractArray{ColumnDefinition}, level = 1)
    unique_names = get_unique_current_names(column_defs, level)
    nodes = Vector{Node}(undef, length(unique_names))
    for (i, unique_name) in enumerate(unique_names)
        nodes[i] = extract_path_node!(csm, column_defs, unique_name, level)
    end
    return nodes
end

"""Analyze the column_defs that match the unique name at this level and create a node"""
function extract_path_node!(csm, column_defs, unique_name, level)
    matching_defs = filter(p -> current_path_name(p, level) == unique_name, column_defs)
    are_value_nodes = [!has_more_keys(def, level) for def in matching_defs]
    
    all_value_nodes = all(are_value_nodes)
    mix_of_node_types = !all_value_nodes && any(are_value_nodes)

    if all_value_nodes
        # If we got to a value node, there should only be one.
        def = first(matching_defs)
        return ValueNode(
            csm, unique_name, get_column_name(def), get_field_path(def), get_pool_arrays(def), NestedIterator(get_default_value(def))
        )
    end

    with_children = view(matching_defs, .!are_value_nodes)
    children_column_defs = make_column_def_child_copies(with_children, unique_name, level)

    child_nodes = make_path_nodes!(csm, children_column_defs, level+1)
    if mix_of_node_types
        without_child_idx = findfirst(identity, are_value_nodes)
        without_child = matching_defs[without_child_idx]
        value_column_node = ValueNode(
            csm,
            unnamed_id, 
            get_column_name(without_child),
            (get_field_path(without_child)..., unnamed), 
            get_pool_arrays(without_child),
            NestedIterator(get_default_value(without_child))
        )
        push!(child_nodes, value_column_node)
    end

    return PathNode(csm, unique_name, child_nodes)
end

"""Create a graph of field_paths that models the structure of the nested data"""
make_path_graph(csm, column_defs) = PathNode(top_level_id, make_path_nodes!(csm, column_defs))
make_path_graph(_, ::Nothing) = SimpleNode(unnamed_id)
end
