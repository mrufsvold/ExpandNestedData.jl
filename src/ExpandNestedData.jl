module ExpandNestedData
using Base: merge!
using DataStructures
using DataStructures: Stack, OrderedRobinDict
using Logging
using PooledArrays
using StructTypes
using SumTypes
using TypedTables: Table

export expand
export ColumnDefinition

"""NameValueContainer is an abstraction on Dict and DataType structs so that we can get their
contents without worrying about `getkey` or `getproperty`, etc.
"""
NameValueContainer = Union{StructTypes.DictType, StructTypes.DataType}
Container = Union{StructTypes.DictType, StructTypes.DataType, StructTypes.ArrayType}
struct ExpandMissing end

function get_name end
function get_id end

include("Utils.jl")
include("NameLists.jl")
include("NestedIterators.jl")
include("ColumnSetManager.jl")
include("ColumnDefinitions.jl")
include("PathGraph.jl")
using .NestedIterators
using .ColumnSetManagers
using .ColumnDefinitions
using .PathGraph
include("ExpandedTable.jl")
include("Core.jl")
include("CoreHelpers.jl")

end
