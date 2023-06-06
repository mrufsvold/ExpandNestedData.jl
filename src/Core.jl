using DataStructures: Stack

"""
    expand(data, column_defs=nothing; 
            default_value = missing, 
            lazy_columns::Bool = false,
            pool_arrays::Bool = false, 
            column_names::Dict = Dict{Tuple, Symbol}(),
            column_style::Symbol=:flat, 
            name_join_pattern = "_")

Expand a nested data structure into a Tables

Args:
* data::Any - The nested data to unpack
* column::Vector{ColumnDefinition} - A list of paths to follow in `data`, ignoring other branches. Optional. Default: `nothing`.
* lazy_columns::Bool - If true, return columns using a lazy iterator. If false, `collect` into regular vectors before returning. Default: `true` (don't collect).
* pool_arrays::Bool - If true, use pool arrays to `collect` the columns. Default: `false`.
* column_names::Dict{Tuple, Symbol} - A lookup to replace column names in the final result with any other symbol
* column_style::Symbol - Choose returned column style from `:nested` or `:flat`. If nested, `column_names` are ignored and a TypedTables.Table is returned in which the columns are nested in the same structure as the source data. Default: `:flat`
* name_join_pattern::String - A pattern to put between the keys when joining the path into a column name. Default: `"_"`.
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
    columns = create_columns(data, path_graph; default_value=default_value)
    
    final_column_defs = column_definitions isa Nothing ? 
        construct_column_definitions(columns, column_names, pool_arrays, name_join_pattern) :
        column_definitions

    return ExpandedTable(columns, final_column_defs; lazy_columns = lazy_columns, pool_arrays = pool_arrays, column_style = typed_column_style, name_join_pattern=name_join_pattern)
end

"""Wrap an object in the correct UnpackStep"""
function wrap_object(name::N, data::T, level::Int64, path_node::C, step_type::S=nothing) where {N,T,C,S}
    if T <: ExpandMissing
        return default_object(name, level, path_node)
    end
    struct_t = typeof(StructTypes.StructType(T))
    obj_type = if S <: StepType
        step_type
    elseif struct_t <: StructTypes.ArrayType
        arr
    elseif struct_t <: NameValueContainer
        dict
    else
        leaf
    end
    return UnpackStep{N,T,C}(obj_type, name, data, level, path_node)
end

# Helper functions for building Unpack steps
default_object(name::N, level, path_node::C) where {N,C} = UnpackStep{N, Nothing, C}(default, name, nothing, level, path_node)
stack_instruction(name::N, col_n, level) where N = UnpackStep{N, Int64, Nothing}(stack_cols, name, col_n, level, nothing)
merge_instruction(name::N, col_n, level) where N = UnpackStep{N, Int64, Nothing}(merge_cols, name, col_n, level, nothing)
column_set_step(cols::T) where T = UnpackStep{Nothing, T, Nothing}(columns, nothing, cols, 0, nothing)


function create_columns(data, path_graph; default_value=missing, kwargs...)
    default_column = NestedIterator(default_value)
    @assert length(default_column) == 1 "The default value must have a length of 1. If you want the value to have a length, try wrapping in a Tuple with `(default_val,)`"
    column_stack = ColumnSet[]
    instruction_stack = Stack{UnpackStep}()
    push!(instruction_stack, wrap_object(:top_level, data, 0, path_graph))

    while !isempty(instruction_stack)
        step = pop!(instruction_stack)
        dispatch_step!(step, default_column, column_stack, instruction_stack)
    end
    @assert length(column_stack) == 1 "Internal Error, more than one column stack resulted"
    return first(column_stack)
end 

function dispatch_step!(step, default_column, column_stack, instruction_stack)
    step_type = get_step_type(step)

    if step_type == columns
        push!(column_stack, get_data(step))
    elseif step_type == default
        level = get_level(step)
        col_set = columnset(default_column, level)
        prepend_name!(col_set, get_name(step), level)
        push!(column_stack, col_set)
    elseif step_type == merge_cols
        merge_cols!(step, column_stack)
    elseif step_type == stack_cols
        stack_cols!(step, column_stack, default_column)
    elseif step_type == dict
        process_dict!(step, instruction_stack)
    elseif step_type == arr
        process_array!(step, instruction_stack)
    elseif step_type == leaf
        process_leaf!(step, instruction_stack)
    end
    return nothing
end

#################
function process_leaf!(step, instruction_stack)
    push!(instruction_stack, column_set_step(init_column_set(step)))
end 


# handle unpacking array-like objects
function process_array!(step::UnpackStep{N,T,C}, instruction_stack) where {N,T,C}
    arr = get_data(step)
    name = get_name(step)
    level = get_level(step)
    path_node = get_path_node(step)
    element_count = length(arr)

    if element_count == 0
        # If we have column defs, but the array is empty, that means we need to make a 
        # missing column_set
        next_step = !(C <: Union{SimpleNode, Nothing}) ?
            column_set_step(make_missing_column_set(path_node, level)) :
            default_object(name, level, path_node)
        push!(instruction_stack, next_step)
        return nothing
    elseif element_count == 1
        push!(instruction_stack, wrap_object(name, first(arr), level, path_node))
        return nothing
    elseif is_value_type(eltype(T))
        push!(instruction_stack, column_set_step(init_column_set(arr, name, level)))
        return nothing
    end

    # Arrays with only value types are a seed to a column
    # Arrays with only container elements will get stacked
    # Arrays with a mix need to be split and processed separately
    is_container_mask = is_container.(arr)
    container_count = sum(is_container_mask)
    no_containers = container_count == 0
    all_containers = container_count == element_count

    if no_containers
        push!(instruction_stack, column_set_step(init_column_set(arr, name, level)))
        return nothing
    end

    # The loose values will need to by merged into the stacked objects below
    if !all_containers
        push!(instruction_stack, merge_instruction(name, 2, level))
        loose_values = [e for (f,e) in zip(is_container_mask, arr) if !f]
        t = typeof(loose_values)
        push!(instruction_stack, wrap_object(:unnamed, loose_values, level+1, path_node, leaf))
    end

    push!(instruction_stack, stack_instruction(name, container_count, level))

    containers = all_containers ? arr : [e for (f,e) in zip(is_container_mask, arr) if f]
    for container in containers
        push!(instruction_stack, wrap_object(name, container, level, path_node))
    end
end

# Unpack a name-value pair object
function process_dict!(step::UnpackStep{N,T,C}, instruction_stack) where {N,T,C}
    data = get_data(step)
    level = get_level(step)
    col_defs_provided = !(C <: Union{Nothing, SimpleNode})
    path_node = get_path_node(step)
    data_names = get_names(data)

    child_nodes = col_defs_provided ? children(path_node) : SimpleNode.(data_names)

    names_num = length(child_nodes)
    if names_num == 0
        push!(instruction_stack, column_set_step(ColumnSet()))
    elseif names_num > 1
        push!(instruction_stack, merge_instruction(get_name(step), length(child_nodes), level))
    end

    for child_node in child_nodes
        name = get_name(child_node)
        # both are always true when unguided
        should_have_child = !(child_node isa ValueNode)
        data_has_name = name in data_names
        child_data = get_value(data, name, ExpandMissing())

        # CASE 1: Expected a child node and found one, unpack it (captures all unguided)
        next_step = if should_have_child && data_has_name
            wrap_object(name, child_data, level+1, child_node)
        # CASE 2: Expected a child node, but don't find it 
        elseif should_have_child && !data_has_name
            column_set_step(make_missing_column_set(child_node, level+1))
        # CASE 3: We don't expect a child node: wrap any value in a new column
        elseif !should_have_child
            wrap_object(name, child_data, level+1, child_node, leaf)
        end
        push!(instruction_stack, next_step)
    end
    return nothing
end

###########

function merge_cols!(step, column_stack)
    col_set = pop!(column_stack)
    multiplier = 1
    for _ in 2:get_data(step)
        new_col_set = pop!(column_stack)
        if length(new_col_set) == 0
            continue
        end
        # Need to repeat each value for all of the values of the previous children
        # to make a product of values
        repeat_each_column!(new_col_set, multiplier)
        multiplier *= column_length(new_col_set)
        merge!(col_set, new_col_set)
    end
    if length(col_set) > 1
        # catch up short columns with the total length for this group
        cycle_columns_to_length!(col_set)
    end
    prepend_name!(col_set, get_name(step), get_level(step))
    push!(column_stack, col_set)
    return nothing
end

function stack_cols!(step, column_stack, default_col)
    columns_to_stack = @view column_stack[end-get_data(step)+1:end]
    prepend_name!.(columns_to_stack, Ref(get_name(step)), get_level(step))
    unique_names = columns_to_stack .|> keys |> Iterators.flatten |> unique
    column_set = ColumnSet()
    for name in unique_names
        # For each unique column name, get that column for the results of processing each element
        # in this array, and then stack them all
        column_set[name] = columns_to_stack         .|>
            (col_set -> get_column(col_set, name, default_col))  |>
            (cols -> foldl(stack, cols))
    end
    deleteat!(column_stack, length(column_stack)-get_data(step)+1:length(column_stack))
    push!(column_stack, column_set)
    return nothing
end

