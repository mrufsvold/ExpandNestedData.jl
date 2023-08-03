import .NestedIterators: RawNestedIterator
import .ColumnSetManagers: ColumnSet, cycle_columns_to_length!, repeat_each_column!, get_first_key, 
                get_total_length, column_length, set_length!, free_column_set!, build_final_column_set
import .PathGraph: make_path_graph, get_children, SimpleNode
import .ColumnDefinitions: construct_column_definitions


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
* `data::Any` - The nested data to unpack
* `column_defs::Vector{ColumnDefinition}` - A list of paths to follow in `data`, ignoring other branches. Optional. Default: `nothing`.
## Kwargs:
* `lazy_columns::Bool` - If true, return columns using a lazy iterator. If false, `collect` into regular vectors before returning. Default: `true` (don't collect).
* `pool_arrays::Bool` - If true, use pool arrays to `collect` the columns. Default: `false`.
* `column_names::Dict{Tuple, Symbol}` - A lookup to replace column names in the final result with any other symbol
* `column_style::Symbol` - Choose returned column style from `:nested` or `:flat`. If nested, `column_names` are ignored 
    and a TypedTables.Table is returned in which the columns are nested in the same structure as the source data. Default: `:flat`
* `name_join_pattern::String` - A pattern to put between the keys when joining the path into a column name. Default: `"_"`.
## Returns
`::NamedTuple` when `column_style = :flat` or `TypedTable.Table` when `column_style = :nested`.
"""
function expand(data, column_definitions=nothing; 
        default_value = missing, 
        lazy_columns::Bool = false,
        pool_arrays::Bool = false, 
        column_names::Dict = Dict{Tuple, Symbol}(),
        column_style::Symbol=:flat, 
        name_join_pattern = "_")
    typed_column_style = get_column_style(column_style)
    csm = ColumnSetManager()
    path_graph = make_path_graph(csm, column_definitions)

    raw_columns = create_columns(data, path_graph, csm, default_value)
    columns = build_final_column_set(csm, raw_columns)

    final_path_graph = column_definitions isa Nothing ?
        make_path_graph(csm, construct_column_definitions(columns, column_names, pool_arrays, name_join_pattern)) :
        path_graph

    expanded_table = ExpandedTable(columns, final_path_graph, csm; lazy_columns=lazy_columns, pool_arrays=pool_arrays)

    final_table = if typed_column_style == flat_columns
        as_flat_table(expanded_table)
    elseif typed_column_style == nested_columns
        as_nested_table(expanded_table)
    end
    return final_table
end

function create_columns(data, path_graph, csm, default_value=missing)
    default_column = RawNestedIterator(csm, default_value)
    @assert length(default_column) == 1 "The default value must have a length of 1. If you want the value to have a length, try wrapping in a Tuple with `(default_val,)`"
    column_stack = ColumnSet[]
    instruction_stack = Stack{UnpackStep}()
    
    push!(instruction_stack, wrap_object(NameList(), data, path_graph))

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
        DictStep(n,d,p) => process_dict!(n, d, p, instruction_stack, csm)
        ArrayStep(n,d,p) => process_array!(n, d, p, instruction_stack, csm)
        LeafStep(n,d) => process_leaf!(n, d, instruction_stack, csm)
        DefaultStep(n) => create_default_column_set!(n, default_column, column_stack, csm)
        MergeStep(d) => merge_cols!(d, column_stack, csm)
        StackStep(d) => stack_cols!(d, column_stack, default_column, csm)
        NewColumnSetStep(cs) => push!(column_stack, cs)
    end
    return nothing
end

"""
    process_leaf!(step, instruction_stack, csm)
Take a value at the end of a path and wrap it in a new ColumnSet
"""
function process_leaf!(name_list, data, instruction_stack, csm)
    push!(instruction_stack, init_column_set_step(csm, name_list, data))
end 

"""
    create_default_column_set!(step, default_column, column_stack, csm)
Build a column set with a single column which is the default column for the run
"""
function create_default_column_set!(name_list, default_column, column_stack, csm)
    name_id = get_id(csm, name_list)
    col_set = get_column_set(csm)
    col_set[name_id] = default_column
    push!(column_stack, col_set)
end

"""
    process_dict!(step::UnpackStep, instruction_stack)

Handle a NameValuePair container (struct or dict) by calling process on all values with a 
new UnpackStep that has a name matching the key. If ColumnDefinitions are provided, then
only grab the keys that apply and add default columns where a key is missing.
"""
function process_dict!(parent_name_list, data, node, instruction_stack, csm)
    data_name_ids = get_id.(Ref(csm), get_names(data))
    @debug "processing NameValueContainer" step_type=:dict  dtype=typeof(data) key_ids=data_name_ids

    child_nodes = @cases node begin
        Path => [c for c in get_children(node) if get_name(c) != unnamed_id]
        Value => throw(ErrorException("Got value node in process_dict, should have been passed to process_leaf"))
        Simple => (SimpleNode(id) for id in data_name_ids)
    end

    if length(child_nodes) == 0
        push!(instruction_stack, empty_column_set_step(csm))
        return nothing
    end

    push!(instruction_stack, MergeStep(length(child_nodes)))

    for child_node in child_nodes
        name_id = get_name(child_node)
        @debug "getting information for child" node=child_node
        name_list = NameList(parent_name_list, name_id)
        # TODO we have to do this lookup twice (once to make id, once to get name back)
        # it would be better to zip up the name_ids with the values as they're constructed
        name = get_name(csm, name_id)
        child_data = get_value(data, name, ExpandMissing())
        @debug "child data retrieved" data=child_data
        data_has_name = name_id in data_name_ids 
        next_step = @cases child_node begin
            Path => wrap_container_val(data_has_name, name_list, child_data, child_node, csm)
            Value => wrap_object(name_list, child_data, child_node, LeafStep)
            Simple => wrap_object(name_list, child_data, child_node)
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
function process_array!(name_list, arr::T, node, instruction_stack, csm) where {T <: AbstractArray}
    element_count = length(arr)::Int64
    @debug "Processing array" dtype=T arr_len=element_count
    if element_count == 0
        # If we have column defs, but the array is empty, that means we need to make a 
        # missing column_set
        @cases node begin
            [Path,Value] => empty_arr_path!(csm, node, instruction_stack)
            Simple => empty_arr_simple!(name_list, instruction_stack)
        end
        return nothing
    elseif element_count == 1
        @cases node begin 
            [Path,Value,Simple] => push!(instruction_stack, wrap_object(name_list, first(arr), node))
        end
        
        return nothing
    elseif all_eltypes_are_values(T)
        push!(instruction_stack, init_column_set_step(csm, name_list, arr))
        return nothing
    end

    # Arrays with only value types are a seed to a column
    # Arrays with only container elements will get stacked
    # Arrays with a mix need to be split and processed separately
    is_container_mask = is_container.(arr)
    container_count = sum(is_container_mask)
    all_containers, no_containers = @cases node begin
        Simple => (container_count == element_count, container_count == 0)
        Value => (false, true)
        Path(_,c) => calculate_container_status_for_path_node(c, container_count)
    end
    @debug "element_types" all_containers=all_containers no_containers=no_containers
    if no_containers
        push!(instruction_stack, init_column_set_step(csm, name_list, arr))
        return nothing
    end

    # The loose values will need to by merged into the stacked objects below
    if !all_containers
        push!(instruction_stack, MergeStep(2))
        loose_values = view(arr, .!is_container_mask)
        next_step = length(loose_values) == 0 ?
            missing_column_set_step(csm, node) :
            wrap_object(NameList(name_list, unnamed_id), loose_values, node, LeafStep)
        @debug "loose values" next_step=next_step
        push!(instruction_stack, next_step)
    end

    container_count > 1 && push!(instruction_stack, StackStep(container_count))

    containers = view(arr, is_container_mask)
    for container in containers
        next_step = wrap_object(name_list, container, node)
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
    multiplier = column_length(col_set)
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

    # Since the column_sets are already sorted by key, the minimum is the first key in a columnset
    # We go down each columnset and check if it has a matching key.
    # From there, we either pop! the column if the key matches or create a default column and add
    # it to the stack
    while !all(length(cs)==0 for cs in columns_to_stack)
        first_key = minimum(get_first_key, columns_to_stack)
        matching_cols = (pop!(cs, first_key, default_col) for cs in columns_to_stack)
        new_column = vcat(matching_cols...)
        push!(new_column_set, first_key=>new_column)
    end

    # free the column_sets that are no longer needed
    for _ in 1:column_set_num
        cs = pop!(column_stack)
        free_column_set!(csm, cs)
    end

    push!(column_stack, new_column_set)
    return nothing
end

