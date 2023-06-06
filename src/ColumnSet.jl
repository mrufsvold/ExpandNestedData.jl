##### ColumnSet #####
#####################

# Convenience alias for a dictionary of columns
ColumnSet = Dict{Tuple, NestedIterator} 
columnset(col, depth) = ColumnSet(Tuple(() for _ in 1:depth) => col)
init_column_set(data, depth) = columnset(NestedIterator(data), depth)
init_column_set(step) = init_column_set(get_data(step), get_name(step), get_level(step))
function init_column_set(data, name, depth)
    col_set = init_column_set(data, depth)
    prepend_name!(col_set, name, depth)
    return col_set
end

column_length(cols) = cols |> values |> first |> length 

"""apply_in_place!(cols, f, args...)
Apply f to each key, column pair by popping the value and readding
the key (this prevents mismatching key hashes after manipulating a ColumnSet)"""
function apply_in_place!(cols, f, args...)
    initial_keys = copy(keys(cols))
    for key in initial_keys 
        val = pop!(cols, key)
        key, val = f(key, val, args...)
        cols[key] = val
    end
end

"""
prepend_name!(cols, name, depth)
Set the given name for all column keys at the given depth
"""
function prepend_name!(cols, name, depth)
    depth < 1 && return nothing
    apply_in_place!(cols, _prepend_name, name, depth)
end
function _prepend_name(key, val, name, depth)
    new_key = Tuple(i==depth ? name : k for (i,k) in enumerate(key))
    return new_key, val
end

"""
repeat_each_column!(cols, n)

Given a column set, apply repeat_each to all columns in place
"""
function repeat_each_column!(cols, n)
    apply_in_place!(cols,_repeat_each_column, n)
end
function _repeat_each_column(key, val, n)
    return key, repeat_each(val, n)
end

"""
cycle_columns_to_length!(cols::ColumnSet) 

Given a column set where the length of all columns is some factor of the length of the longest
column, cycle all the short columns to match the length of the longest
"""
function cycle_columns_to_length!(cols::ColumnSet)
    col_lengths = cols |> values .|> length
    longest = col_lengths |> maximum
    apply_in_place!(cols, cols_to_length, longest)
    return cols
end
function cols_to_length(key, val, longest)
    catchup_mult = longest ÷ length(val)
    return key, cycle(val, catchup_mult)
end

"""
get_column(cols::ColumnSet, name, default::NestedIterator)

Get a column from a set with a given name, if no column with that name is found
construct a new column with same length as column set
"""
get_column(cols::ColumnSet, name, default::NestedIterator) = name in keys(cols) ? cols[name] : cycle(default, column_length(cols) ÷ length(default))



"""Return a missing column for each member of a child path"""
function make_missing_column_set(path_node, current_index)
    missing_column_set =  Dict(
        path_to_value(value_node, current_index) => get_default(value_node)
        for value_node in get_all_value_nodes(path_node)
    )
    return missing_column_set
end


