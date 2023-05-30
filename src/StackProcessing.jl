using DataStructures: Stack
using Base: ImmutableDict

abstract type AbstractObject{N,T} end
struct DictObject{N, T} <: AbstractObject{N,T} 
    name::Symbol
    data::T
    uses_keys::Bool
    level::Int32
end
struct ArrayObject{N, T}  <: AbstractObject{N,T} 
    name::Symbol
    data::T
    level::Int32
end
struct LeafObject{N, T}  <: AbstractObject{N,T} 
    name::N
    data::T
    level::Int32
end
struct DefaultObject{T} <: AbstractObject{Nothing, T} 
    name::T
    level::Int32
end

function wrap_object(name::N, data::T, level::Int32) where {N,T}
    struct_t = StructTypes.StructType(T)
    if struct_t <: NameValueContainer
        uses_keys = struct_t <: StructTypes.DictType
        return DictObject{N,T}(name, data, uses_keys, level)
    elseif struct_t <: StructTypes.ArrayType
        return ArrayObject{N,T}(name, data, level)
    end
    return LeafObject{N,T}(name, data, level)
end

struct Merger{T}
    name::T
    level::Int32
end
struct Stacker{T}
    name::T
end

function create_columns(data; default_value=missing, kwargs...)
    default_column = NestedIterator(default_value)
    column_stack = ColumnSet[]
    instruction_stack = Stack{Any}()
    push!(data, instruction_stack)

    while !isempty(instruction_stack)
        obj = pop!(stack, instruction_stack)
        if obj isa _ColumnSet
            push!(obj, column_stack)
            continue
        end
        if obj isa Stacker 
            run_instruction!(obj, column_stack, default_column)
            continue
        elseif || obj isa Merger
            run_instruction!(obj, column_stack)
            continue
        end
        process_node!(obj, instruction_stack)
    end 
    return first(column_stack)
end 

#################
function process_node!(obj::LeafObject, instruction_stack)
    push!(init_column_set(obj.data, obj.name, obj.level), instruction_stack)
end 


# handle unpacking array-like objects
function process_node!(obj::ArrayObject{N,T}, instruction_stack) where {N,T}
    element_count = length(obj.data)
    if element_count == 0
        push!(DefaultObject{N}(obj.name, obj.level -1), instruction_stack)
        return nothing
    elseif element_count == 1
        push!(wrap_object(obj.name, first(obj.data), obj.level), instruction_stack)
        return nothing
    elseif is_value_type(eltype(T))
        push!(init_column_set(obj.data, obj.name, obj.level-1), instruction_stack)
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
        push!(init_column_set(obj.data, obj.name, obj.level-1), instruction_stack)
        return nothing
    end

    push!(Stacker{N}(obj.name), instruction_stack)

    if !all_containers
        loose_values = [e for (f,e) in zip(is_container_mask,obj.data) if !f]
        t = typeof(loose_values)
        push!(LeafObject{Union{N,Symbol}, t}(:unnamed, loose_values, obj.level), instruction_stack)
    end

    

    containers = all_containers ? obj.data : [e for (f,e) in zip(is_container_mask,obj.data) if f]
    for container in containers
        push!(wrap_object(obj.name, container, obj.level), instruction_stack)
    end
end

# Unpack a name-value pair object
function process_node!(obj::DictObject{N,T}, instruction_stack) where {N,T}
    required_names = get_names(obj)
    push!(Merger{N}(obj.name, obj.level))

    for name in required_names
        child_data = get_value(obj, name, missing)
        push!(wrap_object(name, child_data, obj.level+1), instruction_stack)
    end
end

###########

function run_instruction!(obj::Merger, column_stack)
    col_set = pop!(column_stack)
    multiplier = 1
    for new_col_set in column_stack
        # Need to repeat each value for all of the values of the previous children
        # to make a product of values
        repeat_each_column!(new_col_set, multiplier)
        multiplier *= column_length(new_col_set)
        merge!(columns, new_col_set)
    end
    if length(col_set) > 0
        # catch up short columns with the total length for this group
        cycle_columns_to_length!(col_set)
    end
    prepend_name!(col_set, obj.name, obj.level)

    empty!(column_stack)
    push!(column_stack, col_set)
    return nothing
end

function run_instruction!(obj::Stacker, column_stack, default_col)
    unique_names = column_stack .|> keys |> Iterators.flatten |> unique
    column_set = ColumnSet()
    for name in unique_names
        # For each unique column name, get that column for the results of processing each element
        # in this array, and then stack them all
        column_set[name] = column_stack         .|>
            (col_set -> get_column(col_set, name, default_col))  |>
            (cols -> foldl(stack, cols))
    end
    empty!(column_stack)
    push!(column_stack, column_set)
    return nothing
end

