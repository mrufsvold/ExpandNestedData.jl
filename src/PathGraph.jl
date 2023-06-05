##### PathGraph #####
#####################

abstract type AbstractPathNode end

struct PathNode <: AbstractPathNode
    name
    children::Vector{AbstractPathNode}
end

struct ValueNode <: AbstractPathNode
    name
    children::Vector{AbstractPathNode}
    field_path::Tuple
    pool_arrays
    default::NestedIterator
end

struct SimpleNode <: AbstractPathNode
    name
end
ValueNode(name, field_path, pool_arrays, default) = ValueNode(name, ValueNode[], field_path, pool_arrays,default)

children(n::AbstractPathNode) = n.children
get_name(n::AbstractPathNode) = n.name
field_path(n::ValueNode) = n.field_path
pool_arrays(n::ValueNode) = n.pool_arrays
get_default(n::ValueNode) = n.default

function path_to_children(c::ValueNode, current_index)
    fp = field_path(c)
    return fp[current_index:end]
end

function get_all_value_nodes(node)
    value_node_channel = Channel{ValueNode}() do ch
        get_all_value_nodes(node, ch)
    end
    return collect(value_node_channel)
end
function get_all_value_nodes(node::T, ch) where {T}
    if T <: ValueNode
        put!(ch, node)
        return nothing
    end
    get_all_value_nodes.(children(node), Ref(ch))
    return nothing
end



"""
SIDE EFFECT: also appends :unnamed to any column defs that stop at a pathnode to capture any
loose values in an array at that level
"""
function make_path_nodes!(column_defs, depth = 1)
    unique_names = get_unique_current_names(column_defs, depth)
    nodes = Vector{AbstractPathNode}(undef, length(unique_names))
    for (i, unique_name) in enumerate(unique_names)
        matching_defs = filter(p -> current_path_name(p, depth) == unique_name, column_defs)
        are_value_nodes = [!has_more_keys(def, depth) for def in matching_defs]
        
        all_value_nodes = all(are_value_nodes)
        mix_of_node_types = !all_value_nodes && any(are_value_nodes)

        if all_value_nodes
            # If we got to a value node, there should only be one.
            def = first(matching_defs)
            nodes[i] = ValueNode(
                unique_name, field_path(def), pool_arrays(def), NestedIterator(default_value(def)))
            continue
        end

        with_children = !mix_of_node_types ? 
            matching_defs :
            [def for (is_value, def) in zip(are_value_nodes, matching_defs) if !is_value]
        children_col_defs = make_column_def_child_copies(with_children, unique_name, depth)

        child_nodes = make_path_nodes!(children_col_defs, depth+1)
        if mix_of_node_types
            without_child_idx = findfirst(identity, are_value_nodes)
            without_child = matching_defs[without_child_idx]
            value_column_node = ValueNode(
                :unnamed, 
                (field_path(without_child)..., :unnamed), 
                pool_arrays(without_child),
                NestedIterator(default_value(without_child)))
            push!(child_nodes, value_column_node)
            append_name!(without_child, :unnamed)
        end

        nodes[i] = PathNode(unique_name, child_nodes)
    end
    return nodes
end 


"""Create a graph of field_paths that models the structure of the nested data"""
make_path_graph(col_defs::Vector{ColumnDefinition}) = PathNode(:TOP_LEVEL, make_path_nodes!(col_defs))
make_path_graph(::Nothing) = nothing
