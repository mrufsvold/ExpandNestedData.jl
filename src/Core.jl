"""
    expand(data, column_defs=nothing; 
            default_value = missing, 
            lazy_columns::Bool = false,
            pool_arrays::Bool = false, 
            column_names::Dict = Dict{Tuple, Symbol}(),
            column_style::Symbol=:flat, 
            name_join_pattern = "_")

Expand a nested data structure into a Tables

## Args:
* data::Any - The nested data to unpack
* column_defs::Vector{ColumnDefinition} - A list of paths to follow in `data`, ignoring other branches. Optional. Default: `nothing`.
## Kwargs:
* `lazy_columns::Bool` - If true, return columns using a lazy iterator. If false, `collect` into regular vectors before returning. Default: `true` (don't collect).
* `pool_arrays::Bool` - If true, use pool arrays to `collect` the columns. Default: `false`.
* `column_names::Dict{Tuple, Symbol}` - A lookup to replace column names in the final result with any other symbol
* `column_style::Symbol` - Choose returned column style from `:nested` or `:flat`. If nested, `column_names` are ignored 
    and a TypedTables.Table is returned in which the columns are nested in the same structure as the source data. Default: `:flat`
* `name_join_pattern::String` - A pattern to put between the keys when joining the path into a column name. Default: `"_"`.
## Returns
`::NamedTuple`
"""
function expand(data, column_definitions=nothing; 
        default_value = missing, 
        lazy_columns::Bool = false,
        pool_arrays::Bool = false, 
        column_names::Dict = Dict{Tuple, Symbol}(),
        column_style::Symbol=:flat, 
        name_join_pattern = "_")

    typed_column_style = get_column_style(column_style)
    path_graph = make_path_graph(column_definitions)

    csm = ColumnSetManager()
    raw_columns = create_columns(data, path_graph, csm, default_value)
    columns = build_final_column_set(csm, raw_columns)

    final_path_graph = column_definitions isa Nothing ?
        make_path_graph(construct_column_definitions(columns, column_names, pool_arrays, name_join_pattern)) :
        path_graph

    expanded_table = ExpandedTable(columns, final_path_graph; lazy_columns=lazy_columns, pool_arrays=pool_arrays)

    final_table = if typed_column_style == flat_columns
        as_flat_table(expanded_table)
    elseif typed_column_style == nested_columns
        as_nested_table(expanded_table)
    end
    return final_table
end

"""Wrap an object in the correct UnpackStep"""
function wrap_object(name, data::T, path_node::Node, step_type::S=nothing) where {T,S}
    @debug "running wrap object" dtype=T name=name
    if T <: ExpandMissing
        @debug "got missing path for" name=name
        return UnpackStep'.default(name)
    elseif !(S <: Nothing)
        @debug "enforced step_type" step_type=step_type name=name
        return step_type(name, data, path_node)
    end
    struct_t = typeof(StructTypes.StructType(T))
    @debug "StructType calculated" t=struct_t
    _step_type = if struct_t <: StructTypes.ArrayType
        UnpackStep'.arr
    elseif struct_t <: NameValueContainer
        UnpackStep'.dict
    else
        leaf_step
    end
    @debug "wrapping step" step_type=_step_type
    return _step_type(name, data, path_node)
end

# A couple predefined new column set step creators
missing_column_set_step(csm, path_node) = UnpackStep'.columns(make_missing_column_set(csm, path_node))
init_column_set_step(csm, name, data) = UnpackStep'.columns(init_column_set(csm, name, data))
empty_column_set_step(csm) = UnpackStep'.columns(get_column_set(csm))

function create_columns(data, path_graph, csm, default_value=missing)
    default_column = NestedIterator(default_value)
    @assert length(default_column) == 1 "The default value must have a length of 1. If you want the value to have a length, try wrapping in a Tuple with `(default_val,)`"
    column_stack = ColumnSet[]
    instruction_stack = Stack{UnpackStep}()
    
    push!(instruction_stack, wrap_object(top_level(), data, path_graph))

    while !isempty(instruction_stack)
        step = pop!(instruction_stack)
        dispatch_step!(step, default_column, column_stack, instruction_stack, csm)
    end
    @assert length(column_stack) == 1 "Internal Error, more than one column stack resulted"
    return first(column_stack)
end 



"""
    dispatch_step!(step, default_column, column_stack, instruction_stack)
Generic dispatch to the correct function for this step
"""
function dispatch_step!(step, default_column, column_stack, instruction_stack, csm)
    @debug "dispatching" step=step
    @cases step begin
        dict(n,d,p) => process_dict!(n, d, p, instruction_stack, csm)
        arr(n,d,p) => process_array!(n, d, p, instruction_stack, csm)
        leaf(n,d) => process_leaf!(n, d, instruction_stack, csm)
        merge_cols(d) => merge_cols!(d, column_stack, csm)
        stack_cols(d) => stack_cols!(d, column_stack, default_column, csm)
        default(n) => create_default_column_set!(n, default_column, column_stack, csm)
        columns(cs) => push!(column_stack, cs)
    end
    return nothing
end

"""
    process_leaf!(step, instruction_stack, csm)
Take a value at the end of a path and wrap it in a new ColumnSet
"""
function process_leaf!(name, data, instruction_stack, csm)
    push!(instruction_stack, init_column_set_step(csm, name, data))
end 

"""
    create_default_column_set!(step, default_column, column_stack, csm)
Build a column set with a single column which is the default column for the run
"""
function create_default_column_set!(name, default_column, column_stack, csm)
    col_set = get_column_set(csm)
    name_id = get_id(csm, name)
    col_set[name_id] = default_column
    push!(column_stack, col_set)
end

"""
    process_dict!(step::UnpackStep, instruction_stack)

Handle a NameValuePair container (struct or dict) by calling process on all values with a 
new UnpackStep that has a name matching the key. If ColumnDefinitions are provided, then
only grab the keys that apply and add default columns where a key is missing.
"""
function process_dict!(parent_name, data, wrapped_node, instruction_stack, csm)
    data_names = get_names(data)
    @debug "processing NameValueContainer" step_type=:dict  dtype=typeof(data) keys=data_names

    child_nodes = @cases wrapped_node begin
        [Value, Path](node) => [c for c in get_children(node) if get_name(c) != unnamed()] 
        Simple => (wrap(SimpleNode(n)) for n in data_names)
    end

    names_num = length(child_nodes)
    if names_num == 0
        push!(instruction_stack, empty_column_set_step(csm))
        return nothing
    end

    push!(instruction_stack, UnpackStep'.merge_cols(length(child_nodes)))

    for child_node in child_nodes
        name = get_name(child_node)
        @debug "getting information for child" name=name node=child_node
        wrapped_path = NameList(get_id(csm, name), parent_name)
        child_data = get_value(data, name, ExpandMissing())
        @debug "child data retrieved" data=child_data
        data_has_name = name in data_names 

        next_step = @cases child_node begin
            Path => wrap_container_val(data_has_name, wrapped_path, child_data, child_node, csm)
            Value => wrap_object(wrapped_path, child_data, child_node, leaf_step)
            Simple => wrap_object(wrapped_path, child_data, child_node)
        end
        @debug "Adding next step" child_name=name step=next_step
        push!(instruction_stack, next_step)
    end
    return nothing
end

""" 
process_array!(step::UnpackStep, instruction_stack)
Handle each element of an array
If it is empty, return default value.
If it is all "containers", stack the results.
If it is all "values", return it to be processed as a leaf
If it is a mix, take the loose "values" and process as a leaf. Then merge that ColumnSet with
    the ColumnSet resulting from stacking the containers.
"""
function process_array!(name, arr::T, wrapped_node, instruction_stack, csm) where T
    element_count = length(arr)
    @debug "Processing array" dtype=T arr_len=element_count
    if element_count == 0
        # If we have column defs, but the array is empty, that means we need to make a 
        # missing column_set
        @cases wrapped_node begin
            [Path,Value] => empty_arr_path!(csm, wrapped_node, instruction_stack)
            Simple => empty_arr_simple!(name, instruction_stack)
        end
        return nothing
    elseif element_count == 1
        @cases wrapped_node begin 
            [Path,Value,Simple](n) => push!(instruction_stack, wrap_object(name, first(arr), n))
        end
        
        return nothing
    elseif all_eltypes_are_values(T)
        push!(instruction_stack, init_column_set_step(csm, name, arr))
        return nothing
    end

    # Arrays with only value types are a seed to a column
    # Arrays with only container elements will get stacked
    # Arrays with a mix need to be split and processed separately
    is_container_mask = is_container.(arr)
    container_count = sum(is_container_mask)
    all_containers, no_containers = @cases wrapped_node begin
        Simple => (container_count == element_count, container_count == 0)
        Value => (container_count == element_count, true)
        Path(n) => all_and_no_containers_path_node(n, container_count)
    end
    @debug "element_types" all_containers=all_containers no_containers=no_containers
    if no_containers
        push!(instruction_stack, init_column_set_step(csm, name, arr))
        return nothing
    end

    # The loose values will need to by merged into the stacked objects below
    if !all_containers
        push!(instruction_stack, UnpackStep'.merge_cols(2))
        loose_values = [e for (f,e) in zip(is_container_mask, arr) if !f]
        next_step = length(loose_values) == 0 ?
            missing_column_set_step(csm, wrapped_node) :
            wrap_object(NameList(unnamed_id(), name), loose_values, wrapped_node, leaf_step)
        @debug "loose values" next_step=next_step
        push!(instruction_stack, next_step)
    end

    container_count > 1 && push!(instruction_stack, UnpackStep'.stack_cols(container_count))

    containers = (e for (f,e) in zip(is_container_mask, arr) if f)
    for container in containers
        next_step = wrap_object(name, container, wrapped_node)
        @debug "adding container element" next_step=next_step
        push!(instruction_stack, next_step)
    end
end



###########
"""
    merge_cols!(step, column_stack, csm)
Take N ColumnSets from the column_stack and merge them. This means repeating the values of
each ColumnSet such that you get the Cartesian Product of their join.
"""
function merge_cols!(set_num, column_stack, csm)
    col_set = pop!(column_stack)
    multiplier = 1
    for _ in 2:set_num
        new_col_set = pop!(column_stack)
        if length(new_col_set) == 0
            continue
        end
        # Need to repeat each value for all of the values of the previous children
        # to make a product of values
        repeat_each_column!(new_col_set, multiplier)
        multiplier *= column_length(new_col_set)
        merge!(csm, col_set, new_col_set)
    end
    if length(col_set) > 1
        # catch up short columns with the total length for this group
        cycle_columns_to_length!(col_set)
    end
    push!(column_stack, col_set)
    return nothing
end

"""
    stack_cols!(step, column_stack, default_col, csm)
Take the ColumnSets that were created by processing the elements of an array and stack them together.
If a column name is present in one set but not in the other, then insert a default column.
"""
function stack_cols!(column_set_num, column_stack, default_col, csm)
    columns_to_stack = @view column_stack[end-column_set_num+1:end]

    new_column_set = get_column_set(csm)
    total_len = get_total_length(columns_to_stack)
    set_length!(new_column_set, total_len)

    # Since the column_sets are already sorted by key, the minimum first key in a columnset
    # We go down each columnset and check if it has a matching key.
    # From there, we either pop! the column if the key matches or create a default column and add
    # it to the stack
    column_sets_exhausted = false
    while !column_sets_exhausted
        first_key = minimum(get_first_key, columns_to_stack)
        # todo... we could probably unzip this to avoid iterating twice
        matching_cols = [pop_column!(cs, first_key, default_col) for cs in columns_to_stack]
        push!(new_column_set, first_key=>foldl(stack, matching_cols))
        column_sets_exhausted = all(length(cs)==0 for cs in columns_to_stack)
    end

    # free the column_sets that are no longer needed
    for _ in 1:column_set_num
        cs = pop!(column_stack)
        free_column_set!(csm, cs)
    end

    push!(column_stack, new_column_set)
    return nothing
end

