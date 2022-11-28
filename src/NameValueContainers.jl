NameValueContainer = Union{StructTypes.DictType, StructTypes.DataType}

is_NameValueContainer(t) = typeof(StructTypes.StructType(t)) <: NameValueContainer


"""Check if any elements in an iterator are subtypes of NameValueContainer"""
has_namevaluecontainer_element(itr) = itr .|> StructTypes.StructType .|> (st -> st <: NameValueContainer) |> any


"""Define a pairs iterator for all DataType structs"""
get_pairs(x::T) where T = get_pairs(StructTypes.StructType(T), x)
get_pairs(::StructTypes.DataType, x) = ((p, getproperty(x, p)) for p in fieldnames(typeof(x)))
get_pairs(::StructTypes.DictType, x) = pairs(x)

"""Get the keys/names of any NameValueContainer"""
get_names(x::T) where T = get_names(StructTypes.StructType(T), x)
get_names(::StructTypes.DataType, x) = (n for n in fieldnames(typeof(x)))
get_names(x::Dict) = keys(x)

get_value(x::T, name) where T = get_value(StructTypes.StructType(T), x, name)
get_value(::StructTypes.DataType, x, name) = getproperty(x, name)
get_value(x::Dict, name) = x[name]
