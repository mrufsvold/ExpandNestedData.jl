NameValueContainer = Union{StructTypes.DictType, StructTypes.DataType}
Container = Union{NameValueContainer, StructTypes.ArrayType}

is_container(t) = typeof(StructTypes.StructType(t)) <: Container

@enum TypeKind DICT DATATYPE ARRAY VALUE
function type_kind(@nospecialize(x))
    struct_t = StructTypes.StructType(typeof(x))
    return if struct_t === StructTypes.DictType()
        DICT
    elseif struct_t === StructTypes.ArrayType()
        ARRAY
    elseif struct_t isa StructTypes.DataType
        DATATYPE
    else
        VALUE
    end
end
is_container(x::TypeKind) = x != VALUE
is_value(x::TypeKind) = x == VALUE
is_array(x::TypeKind) = x == ARRAY
is_dict(x::TypeKind) = x == DICT
is_datatype(x::TypeKind) = x == DATATYPE


@enum ColumnStyle flat_columns nested_columns
@enum MaybeBool TRUE FALSE MAYBE
function Base.convert(::Type{MaybeBool}, x::Bool)
    return if x
        TRUE
    else
        FALSE
    end
end
function maybe_and(a, b)
    if a == b
        return a
    elseif a == MAYBE
        return b
    elseif b == MAYBE
        return a
    end
    error("Cannot combine $a and $b")
end

struct CustomMissingValue end
const MISSING = CustomMissingValue()

macro get(dict, key, default)
    quote
        let v = get($(esc(dict)), $(esc(key)), $MISSING)
            if v === $MISSING
                $(esc(default))
            else
                v
            end
        end
    end
end

macro getproperty(obj, key, default)
    quote
        if hasproperty($obj, $key)
            getproperty($obj, $key)
        else
            $default
        end
    end
end

function get_property(obj, key, default)
    if hasproperty(obj, key)
        return getproperty(obj, key)
    else
        return default
    end
end
