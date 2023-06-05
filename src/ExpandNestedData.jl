module ExpandNestedData
using PooledArrays
using StructTypes

export expand
export ColumnDefinition
export nested_columns, flat_columns

include("Utils.jl")
include("NestedIterators.jl")
include("ColumnSet.jl")
include("_ExpandTypes.jl")
include("PathGraph.jl")
include("ExpandedTable.jl")
include("StackProcessing.jl")

end
