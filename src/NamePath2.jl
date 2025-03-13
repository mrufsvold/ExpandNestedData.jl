@auto_hash_equals cache = true struct NamePart
    name
end
NamePart(;name) = name isa NamePart ? name : NamePart(name)
Base.string(np::NamePart) = string(np.name)

@auto_hash_equals struct NamePath
    parts::Vector{NamePart}
    NamePath(v::Vector{NamePart}) = new(v)
    NamePath(parts...) = new([NamePart(;name) for name in parts])
end

function append(np::NamePath, @nospecialize(name))
    new_parts = copy(np.parts)
    push!(new_parts, NamePart(;name))
    return NamePath(new_parts)
end
function Base.string(np::NamePath)
    return join_name_path(np, ".")
end
function Base.getindex(np::NamePath, i::Int)
    return np.parts[i].name
end
function Base.length(np::NamePath)
    return length(np.parts)
end
function Base.lastindex(np::NamePath)
    return length(np.parts)
end
function Base.firstindex(::NamePath)
    return 1
end
function Base.iterate(np::NamePath, state)
    ret = iterate(np.parts, state)
    if isnothing(ret)
        return ret
    end
    return (ret[1].name, ret[2])
end
function Base.iterate(np::NamePath)
    ret = iterate(np.parts)
    if isnothing(ret)
        return ret
    end
    return (ret[1].name, ret[2])
end

function join_name_path(np::NamePath, join_pattern)
    parts = imap(string, np.parts)
    joined = join(parts, join_pattern)
    return Symbol(joined)
end


"""
    get_unique_current_names(name_paths, level)
Get all unique names for the given depth level for a list of `NamePath`s
"""
get_unique_current_names(name_paths, level) = unique((current_path_name(name_path, level) for name_path in name_paths))

current_path_name(name_path::NamePath, level) = name_path.parts[level]
