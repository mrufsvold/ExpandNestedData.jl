abstract type AbstractPathNode end
struct TopLevelNode <: AbstractPathNode
    children::Vector{AbstractPathNode}
end
struct PathNode <: AbstractPathNode
    name
    children::Vector{AbstractPathNode}
end

struct ValueNode <: AbstractPathNode
    name
end

function make_path_nodes(paths)
    unique_names = paths .|> first |> unique
    nodes = Vector{AbstractPathNode}(undef, length(unique_names))
    for (i, unique_name) in enumerate(unique_names)
        matching_paths = filter(p -> first(p) == unique_name, paths)
        are_one_element = length.(matching_paths) .== 1
        if all(are_one_element)
            nodes[i] = ValueNode(unique_name)
            continue
        end

        children_paths = filter(p -> length(p) > 1, matching_paths) .|> (p -> p[2:end])
        if any(are_one_element)
            throw(ArgumentError("The path name $unique_name refers a value in one branch and to nested child(ren): $children_names"))
        end
        nodes[i] = PathNode(unique_name, make_path_nodes(children_paths))
    end
    return nodes
end 

make_path_graph(paths) = TopLevelNode(make_path_nodes(paths))
