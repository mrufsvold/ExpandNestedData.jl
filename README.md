# ExpandNestedData.jl
[![codecov](https://codecov.io/gh/mrufsvold/ExpandNestedData.jl/branch/main/graph/badge.svg?token=LQPXGYX4VC)](https://codecov.io/gh/mrufsvold/ExpandNestedData.jl)
### Documenation

[![Dev](https://img.shields.io/badge/docs-dev-blue.svg)](https://mrufsvold.github.io/ExpandNestedData.jl/dev)


#### Tl;Dr
```julia
using ExpandNestedData 
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

expand(message) |> DataFrame
```

#### Using Column Definitions
Use ColumnDefinitions to tightly control what gets added to the table and how it gets added.
```julia
column_defs = [
    ColumnDefinition([:d]; column_name = :ColumnD),
    ColumnDefinition([:a, :b]; flatten_arrays=true),
    ColumnDefinition([:e, :f]; column_name = :MissingColumn, default_value="Missing branch")
]

expand(message, column_defs) |> DataFrame
```

## Roadmap
- [ ] Support for AbstractTree.jl input (This would enable composability with Gumbo.jl and others)
- [ ] Return a custom Table that allows nested and flattened access to columns
- [ ] Use custom Table as input for compressing tabular data to nested data

