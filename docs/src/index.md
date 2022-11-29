# Normalize.jl
Normalize.jl is a small package that can consume nested data structures like dictionaries of
dictionaries or structs of structs and produce a normalized, Tables.jl-compliant NamedTuple.
It can be used with JSON3.jl, XMLDict.jl, and other packages that parse file formats which are
structured as denormalized data.

## Getting Started
### Install
```@repl
using Pkg
Pkg.add(url="https://github.com/mrufsvold/Normalize.jl")
```
### Basic Usage
Normalize provides a single function `normalize` to flatten out nested data. 

```@example
using Normalize #hide
using JSON3
using DataFrames

message = JSON3.read("""
    {
        "a" : [
            {"b" : 1, "c" : 2},
            {"b" : 2},
            {"b" : [3, 4], "c" : 1},
            {"b" : []}
        ],
        "d" : 4
    }
    """
)

normalize(message) |> DataFrame
```
### Kwarg Options
| Parameter | Description |
| --------- | ----------- |
| `flatten_arrays::Bool` | When a leaf node is an array, should the values be flattened into separate rows or treated as a single value. Default: `true`|
| `default_value::Any` | When a certain key exists in one branch, but not another, what value should be used to fill missing. Default: `missing` |
| `pool_arrays::Bool` | When collecting vectors for columns, choose whether to use PooledArrays instead of Base.Vector |

```@example
using Normalize #hide
using JSON3 #hide
using DataFrames #hide

message = JSON3.read(""" #hide
    { #hide
        "a" : [ #hide
            {"b" : 1, "c" : 2}, #hide
            {"b" : 2}, #hide
            {"b" : [3, 4], "c" : 1}, #hide
            {"b" : []} #hide
        ], #hide
        "d" : 4 #hide
    } #hide
    """ #hide
) #hide

normalize(message; flatten_arrays=true, default_value=nothing, pool_arrays=true) |> DataFrame
```
