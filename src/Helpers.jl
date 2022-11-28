# Link a list of keys into an underscore separted column name
join_names(names) = names .|> string |> (s -> join(s, "_")) |> Symbol


# Convenience alias for a dictionary of columns
ColumnSet = Dict{N where N <: Union{Symbol,Vector{Symbol}}, NestedIterator{T} where T <: Any} 
columnset(col) = ColumnSet(Symbol[] => col)
init_column_set(data, expand_arrays=true) = columnset(NestedIterator(data; expand_arrays))
column_length(cols) = cols |> values |> first |> length 
# Add a name to the front of all names in a set of columns
prepend_name!(cols, name) = cols |> keys .|> (k-> pushfirst!(k, name))
# Check if all the columns in a set are of equal length
all_equal_length(cols) = cols |> values .|> length |> allequal

"""
get_column(cols::ColumnSet, name, default=missing)

Get a column from a set with a given name, if no column with that name is found
construct a new column with same length as column set
"""
get_column(cols::ColumnSet, name, default=missing) = name in keys(cols) ? cols[name] : NestedIterator(default; total_length = column_length(cols))


"""
column_set_product!(cols::ColumnSet)
Repeat values of all columns such that the resulting columns have every product of
the input columns. i.e.
column_set_product!(
    Dict(
        [:a] => [1,2],
        [:b] =? [3,4,5]
    )
)
returns
Dict(
    [:a] => [1,1,1,2,2,2],
    [:b] =? [3,4,5,3,4,5]
)
"""
function column_set_product!(cols::ColumnSet)
    multiplier = 1
    for child_column in values(cols)
        repeat_each!(child_column, multiplier)
        multiplier *= length(child_column)
    end
    cycle_columns_to_length!(cols)
end


"""
cycle_columns_to_length!(cols::ColumnSet) 

Given a column set where the length of all columns is some factor of the length of the longest
column, cycle all the short columns to match the length of the longest
"""
function cycle_columns_to_length!(cols::ColumnSet)
    longest = cols |> values .|> length |> maximum
    for child_column in values(cols)
        catchup_mult = Int(longest / length(child_column))
        cycle!(child_column, catchup_mult)
    end
end
