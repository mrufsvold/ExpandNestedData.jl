using .PathGraph
using .PathGraph: Node, get_all_value_nodes, get_field_path, get_default
using .ColumnSetManagers
using .ColumnSetManagers: ColumnSetManager, get_column_set, get_id_for_path, get_name

@sum_type UnpackStep :hidden begin
    DictStep(::NameList, ::Any, ::Node)
    ArrayStep(::NameList, ::AbstractArray, ::Node)
    LeafStep(::NameList, ::Any)
    DefaultStep(::NameList)
    MergeStep(::Int64)
    StackStep(::Int64)
    NewColumnSetStep(::ColumnSet)
end

DictStep(name_list, data, path_node) = UnpackStep'.DictStep(name_list, data, path_node)
ArrayStep(name_list, arr, path_node) = UnpackStep'.ArrayStep(name_list, arr, path_node)
LeafStep(name_list, data) = UnpackStep'.LeafStep(name_list, data)
DefaultStep(name_list) = UnpackStep'.DefaultStep(name_list)
MergeStep(num_columns) = UnpackStep'.MergeStep(num_columns)
StackStep(num_columns) = UnpackStep'.StackStep(num_columns)
NewColumnSetStep(col_set) = UnpackStep'.NewColumnSetStep(col_set)

# A couple predefined new column set step creators
missing_column_set_step(csm, path_node) = NewColumnSetStep(make_missing_column_set(csm, path_node))
init_column_set_step(csm, name, data) = NewColumnSetSteps(init_column_set(csm, name, data))
empty_column_set_step(csm) = NewColumnSetStep(get_column_set(csm))

function PathGraph.get_name(u::UnpackStep)
    return @cases u begin
        [DictStep,ArrayStep,LeafStep,DefaultStep](n) => n
        [MergeStep,StackStep,NewColumnSetStep] => throw(ErrorException("step has no name"))
    end
end
function get_column_number(u::UnpackStep)
    return @cases u begin
        [MergeStep,StackStep](n) => n
        [DictStep,ArrayStep,LeafStep,DefaultStep,NewColumnSetStep] => throw(
            ErrorException("step does not have a column number"))
    end
end
function get_data(u::UnpackStep)
    return @cases u begin
        [DictStep,ArrayStep,LeafStep](n,d) => d
        [DefaultStep,MergeStep,StackStep,NewColumnSetStep] => throw(ErrorException("step do not have a data field"))
    end
end
function get_path_node(u::UnpackStep)
    return @cases u begin
        [DictStep,ArrayStep](n,d,p) => p
        [LeafStep,DefaultStep,MergeStep,StackStep,NewColumnSetStep] => throw(
            ErrorException("step does not contain a path node"))
    end
end
function ColumnSetManagers.get_column_set(u::UnpackStep)
    return @cases u begin
        [NewColumnSetStep](c) => c
        [LeafStep,DefaultStep,MergeStep,StackStep,DictStep,ArrayStep] => throw(
            ErrorException("Only NewColumnSetStep has a column_set field"))
    end
    node
end

"""Wrap an object in the correct UnpackStep"""
function wrap_object(name::NameList, data::T, path_node::Node, step_type::S=nothing) where {T,S}
    @debug "running wrap object" dtype=T name=name
    if T <: ExpandMissing
        @debug "got missing path for" name=name
        return UnpackStep'.DefaultStep(name)
    elseif !(S <: Nothing)
        @debug "enforced step_type" step_type=step_type name=name
        return step_type(name, data, path_node)
    end
    struct_t = typeof(StructTypes.StructType(T))
    @debug "StructType calculated" t=struct_t
    _step_type = if struct_t <: StructTypes.ArrayType
        UnpackStep'.ArrayStep
    elseif struct_t <: NameValueContainer
        DictStep
    else
        LeafStep
    end
    @debug "wrapping step" step_type=_step_type
    return _step_type(name, data, path_node)
end

function empty_arr_simple!(name, instruction_stack)
    next_step = UnpackStep'.DefaultStep(name)
    push!(instruction_stack, next_step)
end

function empty_arr_path!(csm, path_node, instruction_stack)
    next_step = missing_column_set_step(csm, path_node)
    push!(instruction_stack, next_step)
end

function all_and_no_containers_path_node(node, container_count)
    child_nodes = get_children(node)
    # for path nodes, we need to check if there is :unnamed (indicating that there should be loose values)
    # if so, override all_containers so we check for loose
    if !any(unnamed() == get_name(n) for n in child_nodes)
        # otherwise, we ignore any non-containers
        return (true, false)
    end
    (false, container_count == 0)
end

function wrap_container_val(data_has_name, name_id, data, node, csm)
    @debug "wrap_container val for" data=data 
    if data_has_name
        return wrap_object(name_id, data, node)
    end
    return missing_column_set_step(csm, node)
end

"""Return a missing column for each member of a child path"""
function make_missing_column_set(csm, path_node::Node)
    missing_column_set = get_column_set(csm)

    for value_node in get_all_value_nodes(path_node)
        id = get_field_path(value_node)
        missing_column_set[id] = get_default(value_node)
    end

    return missing_column_set
end
