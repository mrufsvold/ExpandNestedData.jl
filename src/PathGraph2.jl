using SumTypes

@sum_type PathNode :hidden begin
    TopLevelNode(
        children::Vector{PathNode}
    )
    BranchNode(
        name::Any,
        children::Vector{PathNode}
    )
    LeafNode(
        name::Any,
        default_value::Any,
        pool_arrays::PoolArrayOptions,
        dtype::DataType
    )
end
function Node(name, children)
    return PathNode'.BranchNode(name, children)
end
function LeafNode(name_path)
    name = last(name_path)
    default_value = nothing#get_default_value(name_path)
    pool_arrays = get_pool_arrays(name_path)
    dtype = get_dtype(name_path)
    return PathNode'.LeafNode(name, default_value, pool_arrays, dtype)
end

function get_name(node::PathNode)
    return @cases node begin
        [BranchNode, LeafNode](name, _...) => name
        TopLevelNode => error("Can't access name for top level node")
    end
end

function get_children(node::PathNode)
    return @cases node begin
        TopLevelNode(children) => children
        BranchNode(_, children) => children
        LeafNode(_, _, _, _) => nothing
    end
end

function get_default_value(node::PathNode)
    return @cases node begin
        LeafNode(_, _, default_value, _) => default_value
        [BranchNode, TopLevelNode] => error("Can't access default value for non-leaf node")
    end
end
function get_pool_arrays(node::PathNode)
    return @cases node begin
        LeafNode(_, _, _, pool_arrays) => pool_arrays
        [BranchNode, TopLevelNode] => error("Can't access pool array attribute for non-leaf node")
    end
end

function get_pool_arrays(::Any)
    return AUTO
end

function get_dtype(node::PathNode)
    return @cases node begin
        LeafNode(_, _, _, dtype) => dtype
        [BranchNode, TopLevelNode] => error("Can't access dtype for non-leaf node")
    end
end
function get_dtype(::Any)
    return Any
end

function make_path_graph(name_paths)
    children = get_node_children(name_paths, 1)
    return PathNode'.TopLevelNode(children)
end

function get_node_children(name_paths, level)
    children_names = unique(np[level] for np in name_paths)
    return PathNode[get_child_node(name_paths, name, level) for name in children_names]
end

function get_child_node(parent_paths, name, previous_level)
    new_name_paths = filter(np -> np[previous_level] == name, parent_paths)
    if previous_level == length(new_name_paths[1])
        return LeafNode(only(new_name_paths))
    end
    children = get_node_children(new_name_paths, previous_level+1)
    return Node(name, children)
end


