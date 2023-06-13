function empty_arr_simple!(name, instruction_stack)
    next_step = UnpackStep'.default(name)
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

