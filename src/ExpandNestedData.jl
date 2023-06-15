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
export ColumnDefinition

"""NameValueContainer is an abstraction on Dict and DataType structs so that we can get their
contents without worrying about `getkey` or `getproperty`, etc.
"""
NameValueContainer = Union{StructTypes.DictType, StructTypes.DataType}
Container = Union{StructTypes.DictType, StructTypes.DataType, StructTypes.ArrayType}
struct ExpandMissing end

include("Utils.jl")
include("NestedIterators.jl")
using .NestedIterators
include("ColumnSetManager.jl")
using .ColumnSetManagers
include("ColumnDefinitions.jl")
using .ColumnDefinitions
# include("Types.jl")
# include("PathGraph.jl")
# include("ExpandTypes.jl")
# include("ExpandedTable.jl")
# include("Core.jl")
# include("CoreHelpers.jl")
end
