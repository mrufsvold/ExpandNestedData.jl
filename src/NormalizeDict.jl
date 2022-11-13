module NormalizeDict

import Base.Iterators: repeated
using Tables

include("Helpers.jl")
include("ConfiguredColumns.jl")


struct ColumnGenerator{T} <: AbstractArray{T,1}
    generator
    length::Int64
    has_missing::Bool
end
Base.iterate(g::ColumnGenerator) = iterate(g.generator)
Base.iterate(g::ColumnGenerator, state) = iterate(g.generator, state)


function collect_column_generator(c::ColumnGenerator, total_length, vector_type::Type=Vector)
    vec = vector_type{eltype(c)}(undef, total_length)
    for (i, v) in zip(eachindex(vec), Iterators.cycle(c))
        vec[i] = v
    end
    return vec
end


repeat_generator(values, repeat_count) = (v for value in values for v in repeated(value, repeat_count))
function repeat_generator(values::ColumnGenerator, repeat_count)
    return ColumnGenerator{eltype(values)}(
        (v for value in values.generator for v in repeated(value, repeat_count)),
        values.length * repeat_count,
        values.has_missing
    )
end


# Make Leaf Nodes
function make_path_graph(values::A, expand::Bool, left_siblings_product, has_missing=false, default = missing) where A <: AbstractArray
    node = if expand
        nonempty_array = length(values) == 0 ? repeated(default, 1) : values
        ColumnGenerator{eltype(nonempty_array)}(repeat_generator(nonempty_array, left_siblings_product), length(nonempty_array), has_missing)
    else
        ColumnGenerator{eltype(values)}(repeated(values, left_siblings_product), 1, has_missing)
    end
    return node
end
function make_path_graph(value, ::Bool, left_siblings_product, has_missing=false, _ = missing)
    return  ColumnGenerator{typeof(value)}(repeated(value, left_siblings_product), 1, has_missing)
end


end # module NormalizeDict
