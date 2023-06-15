module ExpandNestedData
using Base: merge!
using DataStructures
using DataStructures: Stack, OrderedRobinDict, cons, Cons, Nil
using Logging
using PooledArrays
using StructTypes
using SumTypes
using TypedTables: Table

# export expand
# export ColumnDefinition
# export nested_columns, flat_columns

include("NestedIterators.jl")
using .NestedIterators
include("ColumnSetManager.jl")
using .ColumnSetManagers
# include("Utils.jl")
# include("Types.jl")
# include("PathGraph.jl")
# include("ColumnSet.jl")
# include("ExpandTypes.jl")
# include("ExpandedTable.jl")
# include("Core.jl")
# include("CoreHelpers.jl")
end
