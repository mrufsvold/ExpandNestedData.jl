struct ColumnDefinition
    name_path::NamePath
    column_name::Symbol
    default_value
    pool_arrays::MaybeBool
    name_join_pattern::String
end

function ColumnDefinition(name_parts; name_join_pattern = "_", column_name=nothing, default_value=missing, pool_arrays=FALSE)
    name_path = NamePath(name_parts...)
    if isnothing(column_name)
        column_name = join_name_path(name_path, name_join_pattern)
    end
    return ColumnDefinition(name_path, Symbol(column_name), default_value, pool_arrays, name_join_pattern)
end

Base.getindex(cd::ColumnDefinition, i) = get_name_path(cd)[i]
Base.length(cd::ColumnDefinition) = length(get_name_path(cd))
get_name_path(cd::ColumnDefinition) = cd.name_path

make_path_graph(v::AbstractVector{ColumnDefinition}) = make_path_graph(get_name_path.(v))


struct Column
    name::NamePath
    data::IterCapture
end
Base.length(c::Column) = length(c.data)
Base.eltype(c::Column) = eltype(c.data)
function Base.collect(c::Column; pool_arrays)
    if pool_arrays == MAYBE
        # I am picking 1/5 as the threshold for the amount
        # of unique seeds allowed before we switch over to
        # normal arrays. This is made up and should be investigated
        pool_up_to = length(c) ÷ 5
        seeds = get_all_seeds(c.data, pool_up_to)
        if !isnothing(seeds)
            return PooledArray(c.data)
        end
    elseif pool_arrays == TRUE
        return PooledArray(c.data)
    end
    return collect(c.data)
end
function get_name_path(c::Column)
    return c.name
end

function new_column_set(np::NamePath, @nospecialize(data))
    Column[Column(np, seed(data))]
end
function new_column_set_from_vec(np::NamePath, @nospecialize(data), @nospecialize(default_value))
    Column[Column(np, seed_vector(data, default_value))]
end

function concat(columns::Vector{Column}; config)
    iter_caps = imap(c -> c.data, columns)
    data = concat(iter_caps)
    one_name = columns[1].name
    name_path = if config.use_xpath_names == true
        NamePath([
            NamePart(
                one_name[i],
                mapreduce(c -> get_is_array(c.name.parts[i]), maybe_and, columns)
            )
            for i in eachindex(one_name)
        ])
    else
        one_name
    end
    return Column(name_path, data)
end


function cycle_column(column, n)
	return Column(column.name, cycle(column.data, n))
end

function stack_columns(column_sets, name; config)
	return concat(map(c -> get_column(c, name, config.default_value), column_sets); config)
end


function get_column(column_set, name_path, default_value)
	len = length(column_set[1])
	i = findfirst(c -> c.name == name_path, column_set)
	if isnothing(i)
		return cycle_column(Column(name_path, seed(default_value)), len)
	end
	return column_set[i]
end
