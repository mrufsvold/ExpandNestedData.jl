module Normalize
using PooledArrays
using StructTypes

export normalize
export ColumnDefinition

# Link a list of keys into an underscore separted column name
join_names(names) = names .|> string |> (s -> join(s, "_")) |> Symbol

include("ExpandTypes.jl")
include("ExpandedTable.jl")
include("UnguidedProcessing.jl")
include("ConfiguredProcessing.jl")

end
