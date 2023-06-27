module ExpandNestedData
using PooledArrays
using StructTypes
using Base: merge!
using DataStructures
using DataStructures: Stack, OrderedRobinDict, list, cons, Cons, Nil, IntSet
# using Tables
using TypedTables: Table

export expand
export ColumnDefinition
export nested_columns, flat_columns

include("Utils.jl")
include("NestedIterators.jl")
include("ColumnSet.jl")
include("ColumnSetManager.jl")
include("ExpandTypes.jl")
include("PathGraph.jl")
include("ExpandedTable.jl")
include("Core.jl")

end
