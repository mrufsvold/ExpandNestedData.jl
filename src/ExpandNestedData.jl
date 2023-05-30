module ExpandNestedData
using PooledArrays
using StructTypes

export expand
export ColumnDefinition
export nested_columns, flat_columns

include("Utils.jl")
include("ExpandTypes.jl")
include("ExpandedTable.jl")
include("StackProcessing.jl")
include("Processing.jl")

end
