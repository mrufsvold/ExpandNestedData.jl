struct ColumnDefinition
    name_path::NamePath
    column_name::Symbol
    default_value
    pool_arrays::PoolArrayOptions
end

function ColumnDefinition(name_parts; column_name=nothing, default_value=missing, pool_arrays=NEVER)
    name_path = NamePath(name_parts...)
    if isnothing(column_name)
        column_name = join_name_path(name_parts, "_")
    end
    return ColumnDefinition(name_path, Symbol(column_name), default_value, pool_arrays)
end


struct Column
    name::NamePath
    data::IterCapture
end
Base.length(c::Column) = length(c.data)
Base.eltype(c::Column) = eltype(c.data)
function Base.collect(c::Column; pool_arrays)
    if pool_arrays == AUTO
        # I am picking 1/5 as the threshold for the amount
        # of unique seeds allowed before we switch over to
        # normal arrays. This is made up and should be investigated
        pool_up_to = length(c) ÷ 5
        seeds = get_all_seeds(c.data, pool_up_to)
        if !isnothing(seeds)
            return PooledArray(c.data)
        end
    elseif pool_arrays == ALWAYS
        return PooledArray(c.data)
    end
    return collect(c.data)
end
function get_name_path(c::Column)
    return c.name
end
