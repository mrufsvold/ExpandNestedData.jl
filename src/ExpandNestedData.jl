module ExpandNestedData
using PooledArrays
using StructTypes

export expand
export ColumnDefinition
export nested_columns, flat_columns

include("Utils.jl")
include("NestedIterators.jl")
include("ColumnSet.jl")
include("ExpandTypes.jl")
include("PathGraph.jl")
include("ExpandedTable.jl")
include("Core.jl")

end
