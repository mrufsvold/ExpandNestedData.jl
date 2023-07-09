module ExpandNestedData
using Base: merge!
using DataStructures
using DataStructures: Stack, OrderedRobinDict
using Logging
using PooledArrays
using StructTypes
using SumTypes
using TypedTables: Table

export expand, ColumnDefinition

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
import .ColumnDefinitions: ColumnDefinition
include("PathGraph.jl")
include("ExpandedTable.jl")
include("CoreHelpers.jl")

# Here is where the main logic starts
include("Core.jl")

end
