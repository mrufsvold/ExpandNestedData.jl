module NormalizeDict
using PooledArrays
using StructTypes
using Logging
import Base.Iterators: repeated, flatten

include("NameValueContainers.jl")
include("NestedIterators.jl")
include("Helpers.jl")
include("UnguidedProcessing.jl")
include("ConfiguredProcessing.jl")

end
