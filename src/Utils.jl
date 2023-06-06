"""Check if any elements in an iterator are subtypes of NameValueContainer"""
function has_namevaluecontainer_element(itr)
    if eltype(itr) == Any
        return itr .|> eltype .|> is_NameValueContainer |> any
    else
        return itr |> eltype |> get_member_types .|> is_NameValueContainer |> any
    end
end
get_member_types(::Type{T}) where T = T isa Union ? Base.uniontypes(T) : [T]


"""Get the keys/names of any NameValueContainer"""
@generated function get_names(x::T) where T
    struct_t = StructTypes.StructType(T)
    if struct_t isa StructTypes.DataType
        return :((n for n in fieldnames(T)))
    elseif struct_t isa StructTypes.DictType
        return :(keys(x))
    end
    return :(TypeError(:get_names, "Expected a dict or struct", NameValueContainer, T))
end

"""Get the value for a key of any NameValueContainer. If it does not have the key, return default"""
@generated function get_value(x::T, name, default) where T
    struct_t = StructTypes.StructType(T)
    if struct_t isa StructTypes.DataType
        return :(hasproperty(x, name) ? getproperty(x, name) : default)
    elseif struct_t isa StructTypes.DictType
        return :(get(x, name, default))
    end
    return :(TypeError(:get_names, "Expected a dict or struct", NameValueContainer, T))
end

"""Link a list of keys into an underscore separted column name"""
join_names(names, joiner="_") = names .|> string |> (s -> join(s, joiner)) |> Symbol

