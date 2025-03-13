NameValueContainer = Union{StructTypes.DictType, StructTypes.DataType}
Container = Union{NameValueContainer, StructTypes.ArrayType}

is_container(t) = typeof(StructTypes.StructType(t)) <: Container

@enum ColumnStyle flat_columns nested_columns
@enum PoolArrayOptions NEVER ALWAYS AUTO

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
