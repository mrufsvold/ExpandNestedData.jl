using SumTypes

@sum_type PathNode :hidden begin
    NothingNode()
    TopLevelNode(
        children::Vector{PathNode}
    )
    BranchNode(
        name::Any,
        children::Vector{PathNode}
    )
    LeafNode(
        name::Any,
        column_definition::ColumnDefinition
    )
end
function Node(name, children)
    return PathNode'.BranchNode(name, children)
end
function LeafNode(name_path::NamePath; kwargs...)
    LeafNode(ColumnDefinition(name_path); kwargs...)
end
function LeafNode(column_definition::ColumnDefinition; _...)
    name = last(get_name_path(column_definition))
    return PathNode'.LeafNode(name, column_definition)
end
function NothingNode()
    return PathNode'.NothingNode()
end

function get_name(node::PathNode)
    return @cases node begin
        [BranchNode, LeafNode](name, _...) => name
        [TopLevelNode, NothingNode] => error("Can't access name for top level or nothing node")
    end
end

function get_child_by_name(node::PathNode, name)
    children = get_children(node)
    i = findfirst(child -> get_name(child) == name, children)
    isnothing(i) && error("Could not find child with name $name")
    return children[i]
end

function get_children(node::PathNode)
    return @cases node begin
        TopLevelNode(children) => children
        BranchNode(_, children) => children
        [LeafNode, NothingNode] => nothing
    end
end

function get_default_value(node::PathNode)
    return @cases node begin
        LeafNode(_, column_definition) => column_definition.default_value
        [BranchNode, TopLevelNode] => error("Can't access default value for non-leaf node")
    end
end

function get_column_definitions(node::PathNode)
    return @cases node begin
        LeafNode(_, column_definition) => [column_definition]
        NothingNode => error("NothingNode has no leaves")
        [BranchNode, TopLevelNode] => mapreduce(
            get_column_definitions,
            vcat,
            get_children(node),
            init = ColumnDefinition[]
        )
    end
end

function make_path_graph(name_paths; kwargs...)
    children = get_node_children(name_paths, 1; kwargs...)
    return PathNode'.TopLevelNode(children)
end
make_path_graph(::Nothing; kwargs...) = NothingNode()

function get_node_children(name_paths, level; kwargs...)
    children_names = unique(np[level] for np in name_paths)
    return PathNode[get_child_node(name_paths, name, level; kwargs...) for name in children_names]
end

function get_child_node(parent_paths, name, previous_level; kwargs...)
    new_name_paths = filter(np -> np[previous_level] == name, parent_paths)
    if previous_level == length(new_name_paths[1])
        return LeafNode(only(new_name_paths); kwargs...)
    end
    children = get_node_children(new_name_paths, previous_level+1; kwargs...)
    return Node(name, children)
end


