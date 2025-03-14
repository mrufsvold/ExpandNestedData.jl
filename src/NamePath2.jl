@auto_hash_equals cache = true fields = (name,) struct NamePart
    name
    is_array::MaybeBool
end
function NamePart(;name, is_array)
    if name isa NamePart
        name = name.name
    end
    return NamePart(name, is_array)
end
get_is_array(np::NamePart) = np.is_array

Base.string(np::NamePart) = string(np.name)

@auto_hash_equals struct NamePath
    parts::Vector{NamePart}
    NamePath(v::Vector{NamePart}) = new(v)
    NamePath(parts...) = new([NamePart(;name, is_array=false) for name in parts])
end

function append(np::NamePath, @nospecialize(name); is_array)
    new_parts = copy(np.parts)
    push!(new_parts, NamePart(;name, is_array))
    return NamePath(new_parts)
end

function mark_n_as_array(np::NamePath, n::Int)
    new_parts = copy(np.parts)
    new_parts[n] = NamePart(;name = new_parts[n], is_array = TRUE)
    return NamePath(new_parts)
end
function mark_last_as_array(np::NamePath)
    if isempty(np.parts)
        return np
    end
    return mark_n_as_array(np, length(np.parts))
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
function Base.eachindex(np::NamePath)
    return eachindex(np.parts)
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



function collapse_names(name_paths::Vector{NamePath})
    if length(name_paths) <= 1
        return name_paths
    end

    unique_nps = unique(name_paths)
    for i in eachindex(unique_nps)
        np = unique_nps[i]
        group = ifilter(==(np), name_paths)
        for j in eachindex(np)
            np.parts[j] = NamePart(np[j].name,
                mapreduce(x -> get_is_array(x[j]), maybe_and, group)
            )
        end
        # need to rebuild because of the hash since we mutated in place
        unique_nps[i] = NamePath(np.parts)
    end
    return unique_nps
end



"""
    get_unique_current_names(name_paths, level)
Get all unique names for the given depth level for a list of `NamePath`s
"""
get_unique_current_names(name_paths, level) = unique((current_path_name(name_path, level) for name_path in name_paths))

current_path_name(name_path::NamePath, level) = name_path.parts[level]
