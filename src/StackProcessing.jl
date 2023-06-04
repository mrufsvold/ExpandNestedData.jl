using DataStructures: Stack
using Base: ImmutableDict

abstract type AbstractObject{N,T} end
struct DictObject{N, T} <: AbstractObject{N,T} 
    name::Symbol
    data::T
    uses_keys::Bool
    level::Int64
end
struct ArrayObject{N, T}  <: AbstractObject{N,T} 
    name::Symbol
    data::T
    level::Int64
end
struct LeafObject{N, T}  <: AbstractObject{N,T} 
    name::N
    data::T
    level::Int64
end
struct DefaultObject{T} <: AbstractObject{Nothing, T} 
    name::T
    level::Int64
end

function wrap_object(name::N, data::T, level::Int64) where {N,T}
    struct_t = StructTypes.StructType(T)
    if struct_t isa NameValueContainer
        uses_keys = struct_t isa StructTypes.DictType
        return DictObject{N,T}(name, data, uses_keys, level)
    elseif struct_t isa StructTypes.ArrayType
        return ArrayObject{N,T}(name, data, level)
    end
    return LeafObject{N,T}(name, data, level)
end

struct Merger{T}
    name::T
    level::Int64
    n::Int64
end
struct Stacker{T}
    name::T
    level::Int64
    n::Int64
end

function create_columns(data, column_defs; default_value=missing, kwargs...)
    default_column = NestedIterator(default_value)
    @assert length(default_column) == 1 "The default value must have a length of 1. If you want the value to have a length, try wrapping in a Tuple with `(default_val,)`"
    column_stack = ColumnSet[]
    instruction_stack = Stack{Any}()
    push!(instruction_stack, wrap_object(:top_level, data, 0))

    while !isempty(instruction_stack)
        obj = pop!(instruction_stack)
        if obj isa ColumnSet
            push!(column_stack, obj)
            continue
        elseif obj isa DefaultObject
            col_set = columnset(default_column, obj.level)
            prepend_name!(col_set, obj.name, obj.level)
            push!(column_stack, col_set)
            continue
        elseif obj isa Stacker 
            run_instruction!(obj, column_stack, default_column)
            continue
        elseif obj isa Merger
            run_instruction!(obj, column_stack)
            continue
        end
        process_node!(obj, instruction_stack)
    end
    @assert length(column_stack) == 1 "Internal Error, more than one column stack resulted"
    return first(column_stack)
end 

#################
function process_node!(obj::LeafObject, instruction_stack)
    push!(instruction_stack, init_column_set(obj.data, obj.name, obj.level))
end 


# handle unpacking array-like objects
function process_node!(obj::ArrayObject{N,T}, instruction_stack) where {N,T}
    element_count = length(obj.data)
    if element_count == 0
        push!(instruction_stack, DefaultObject{N}(obj.name, obj.level))
        return nothing
    elseif element_count == 1
        push!(instruction_stack, wrap_object(obj.name, first(obj.data), obj.level))
        return nothing
    elseif is_value_type(eltype(T))
        push!(instruction_stack, init_column_set(obj.data, obj.name, obj.level))
        return nothing
    end

    # Arrays with only value types are a seed to a column
    # Arrays with only container elements will get stacked
    # Arrays with a mix need to be split and processed separately
    is_container_mask = is_container.(obj.data)
    container_count = sum(is_container_mask)
    no_containers = container_count == 0
    all_containers = container_count == element_count

    if no_containers
        push!(instruction_stack, init_column_set(obj.data, obj.name, obj.level))
        return nothing
    end

    # The loose values will need to by merged into the stacked objects below
    if !all_containers
        push!(instruction_stack, Merger(obj.name, obj.level, 2))
        loose_values = [e for (f,e) in zip(is_container_mask,obj.data) if !f]
        t = typeof(loose_values)
        push!(instruction_stack, LeafObject{Union{N,Symbol}, t}(:unnamed, loose_values, obj.level+1))
    end

    push!(instruction_stack, Stacker(obj.name, obj.level, container_count))

    containers = all_containers ? obj.data : [e for (f,e) in zip(is_container_mask,obj.data) if f]
    for container in containers
        push!(instruction_stack, wrap_object(obj.name, container, obj.level))
    end
end

# Unpack a name-value pair object
function process_node!(obj::DictObject{N,T}, instruction_stack) where {N,T}
    required_names = get_names(obj.data)
    names_num = length(required_names)
    if names_num == 0
        push!(instruction_stack, ColumnSet())
    elseif names_num > 1
        push!(instruction_stack, Merger{N}(obj.name, obj.level, length(required_names)))
    end
    for name in required_names
        child_data = get_value(obj.data, name, missing)
        push!(instruction_stack, wrap_object(name, child_data, obj.level+1))
    end
end

###########

function run_instruction!(obj::Merger, column_stack)
    col_set = pop!(column_stack)
    multiplier = 1
    for _ in 2:obj.n
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
    prepend_name!(col_set, obj.name, obj.level)
    push!(column_stack, col_set)
    return nothing
end

function run_instruction!(obj::Stacker, column_stack, default_col)
    columns_to_stack = @view column_stack[end-obj.n+1:end]
    prepend_name!.(columns_to_stack, Ref(obj.name), obj.level)
    unique_names = columns_to_stack .|> keys |> Iterators.flatten |> unique
    column_set = ColumnSet()
    for name in unique_names
        # For each unique column name, get that column for the results of processing each element
        # in this array, and then stack them all
        column_set[name] = columns_to_stack         .|>
            (col_set -> get_column(col_set, name, default_col))  |>
            (cols -> foldl(stack, cols))
    end
    deleteat!(column_stack, length(column_stack)-obj.n+1:length(column_stack))
    push!(column_stack, column_set)
    return nothing
end

