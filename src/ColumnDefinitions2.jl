struct ColumnDefinition
    name_path::NamePath
    column_name::Symbol
    default_value
    pool_arrays::Bool
end

function ColumnDefinition(name_parts; column_name=nothing, default_value=missing, pool_arrays=false)
    name_path = NamePath(name_parts...)
    if isnothing(column_name)
        column_name = join_name_path(name_parts, "_")
    end
    return ColumnDefinition(name_path, Symbol(column_name), default_value, pool_arrays)
end
