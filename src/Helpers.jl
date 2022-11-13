join_path_names(path) = path .|> string |> (names -> join(names, "_")) |> Symbol


function find_unique_keys(column_defs)
    # First pull out all the paths 
    paths = getproperty.(column_defs, :field_path)
    keys = (# Get unique keys from the paths 
        names = unique(first.(paths)),
        # Find which keys point to further nesting
        has_children = paths |> pfilter(more_than_one_el) .|> first |> unique)
    return keys
end


function flatten_generators(children, column_name)
    union_type = Union{(eltype(child.generators[column_name]) for child in children)...}
    return ColumnGenerator{union_type}( 
        Iterators.flatten(child.generators[column_name].generator for child in children),
        sum(child.generators[column_name].length for child in children),
        any(child.generators[column_name].has_missing for child in children)
    )
end

pfilter(f) = arr -> filter(f, arr)
first_path_name_is(sym) = col_obj -> first(field_path(col_obj)) == sym
more_than_one_el(arr) = length(arr) > 1
