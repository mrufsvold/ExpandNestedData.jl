# helper functions for inspecting types for container traits
is_container(t) = typeof(StructTypes.StructType(t)) <: Container
is_value_type(t::Type) = !is_container(t) && isconcretetype(t)

"""Check if the eltype of a T are all value types (i.e. not containers)"""
all_eltypes_are_values(::Type{T}) where T = all_is_value_type(eltype(T))
function all_is_value_type(::Type{T}) where T
    if T isa Union
        return all(is_value_type.(Base.uniontypes(T)))
    end
    return is_value_type(T)
end


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


"""Collect an iterator into a tuple"""
collect_tuple(itr) = _collect_tuple(safe_peel(itr))
_collect_tuple(peel_return) = _collect_tuple(peel_return...)
_collect_tuple(::Nothing) = ()
_collect_tuple(val, rest) = (val, collect_tuple(rest)...)

"""safe_peel(itr)
Acts like Base.Iterators.peel, but 1) returns views instead of an Iterator.Rest and
works across all Julia 1.X versions for empty containers
"""
function safe_peel(itr)
    len = length(itr)
    if len == 0
        return nothing
    elseif len == 1
        return (Iterators.only(itr), ())
    end
    return (first(itr), @view itr[2:end])
end


"""make_switch(fs, lengths)
Create a switching function that takes an integer `i` and compares it against 
each length provided in order. Once it accumulates the sum of lengths greater than or equal to 
`i`, it subtracts the previous total length and runs the corresponding function.
"""
function make_switch(fs, lengths)
    func_def = compose_switch_body(fs,lengths)
    @eval $func_def
end

function compose_switch_body(fs,lengths)
    total_len = sum(lengths)

    _fs = Iterators.Stateful(fs)
    _lengths = Iterators.Stateful(lengths)
    
    l = popfirst!(_lengths)
    if_stmt = :(
        if i <= $(l) 
            $(popfirst!(_fs))(i)
        end
    )
    
    curr_stmt = if_stmt
    prev_l = l
    for (f,l) in zip(_fs, _lengths)
        curr_l = l + prev_l
        ex = Expr(
            :elseif, 
            :(i <= $(curr_l)),
            :($f(i-$prev_l))
        )
        push!(curr_stmt.args, ex)
        prev_l = curr_l
        curr_stmt = ex
    end
    
    name = gensym("unvcat_switch")
    error_str = "Attempted to access $total_len-length vector at index "
    func_def = :(
        function $(name)(i)
            i > $(total_len) && error($error_str * "$i")
            $if_stmt
        end
    )
    return func_def
end
