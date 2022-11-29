# Normalize.jl

### Documenation
[![Stable](https://img.shields.io/badge/docs-stable-blue.svg)](https://mrufsvold.github.io/Normalize.jl/stable)
[![Dev](https://img.shields.io/badge/docs-dev-blue.svg)](https://mrufsvold.github.io/Normalize.jl/dev)

#### TL;DR
```julia
using Normalize 
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

#### Using Column Definitions
Use ColumnDefinitions to tightly control what gets added to the table and how it gets added.
```julia
column_defs = [
    ColumnDefinition([:d]; column_name = :ColumnD),
    ColumnDefinition([:a, :b]; flatten_arrays=true),
    ColumnDefinition([:e, :f]; column_name = :MissingColumn, default_value="Missing branch")
]

normalize(message, column_defs) |> DataFrame
```