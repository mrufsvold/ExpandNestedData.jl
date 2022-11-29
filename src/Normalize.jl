module Normalize
using PooledArrays
using StructTypes
import Base.Iterators: repeated, flatten

export normalize
export ColumnDefinition

include("NameValueContainers.jl")
include("NestedIterators.jl")
include("Helpers.jl")
include("UnguidedProcessing.jl")
include("ConfiguredProcessing.jl")

end
