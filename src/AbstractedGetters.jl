"""Define a pairs iterator for all DataType structs"""
get_pairs(x::T) where T = get_pairs(StructTypes.StructType(T), x)
get_pairs(::StructTypes.DataType, x) = ((p, getproperty(x, p)) for p in fieldnames(typeof(x)))
get_pairs(::StructTypes.DictType, x) = pairs(x)

"""Get the keys/names of any NameValuePairsObject object"""
get_names(x::T) where T = get_names(StructTypes.StructType(T), x)
get_names(::StructTypes.DataType, x) = (n for n in fieldnames(typeof(x)))
get_names(x::Dict) = keys(x)
