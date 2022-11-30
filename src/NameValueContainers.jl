NameValueContainer = Union{StructTypes.DictType, StructTypes.DataType}

is_NameValueContainer(t) = typeof(StructTypes.StructType(t)) <: NameValueContainer


"""Check if any elements in an iterator are subtypes of NameValueContainer"""
function has_namevaluecontainer_element(itr)
    if eltype(itr) == Any
        return itr .|> eltype .|> is_NameValueContainer |> any
    else
        return itr |> eltype |> get_member_types .|> is_NameValueContainer |> any
    end
end
get_member_types(T) = T isa Union ? Base.uniontypes(T) : [T]

"""Define a pairs iterator for all DataType structs"""
get_pairs(x::T) where T = get_pairs(StructTypes.StructType(T), x)
get_pairs(::StructTypes.DataType, x) = ((p, getproperty(x, p)) for p in fieldnames(typeof(x)))
get_pairs(::StructTypes.DictType, x) = pairs(x)

"""Get the keys/names of any NameValueContainer"""
get_names(x::T) where T = get_names(StructTypes.StructType(T), x)
get_names(::StructTypes.DataType, x) = (n for n in fieldnames(typeof(x)))
get_names(::StructTypes.DictType, x) = keys(x)

get_value(x::T, name) where T = get_value(StructTypes.StructType(T), x, name)
get_value(::StructTypes.DataType, x, name) = getproperty(x, name)
get_value(::StructTypes.DictType, x, name) = x[name]
