# ExpandNestedData.jl
ExpandNestedData.jl is a small package that can consume nested data structures like dictionaries of
dictionaries or structs of structs and produce a normalized, Tables.jl-compliant NamedTuple.
It can be used with JSON3.jl, XMLDict.jl, and other packages that parse file formats which are
structured as denormalized data.

```@contents
Depth = 4
```

## Getting Started
### Install
```@repl
using Pkg
Pkg.add(url="https://github.com/mrufsvold/ExpandNestedData.jl")
```
### Basic Usage
ExpandNestedData provides a single function `expand` to flatten out nested data. 

```@example
using ExpandNestedData #hide
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
## Configuring Options
While `exapnd` can produce a `Table` out of the box, it is often useful to configure
some options in how it handles the normalization process. `ExpandNestedData.jl` offers two ways to set
these configurations. You can set them globally with `kwargs` or exercise finer control with
per-column configurations.
### Keyword Arguments
| Parameter | Description |
| --------- | ----------- |
| `flatten_arrays::Bool`                        | When a leaf node is an array, should the values be flattened into separate rows or treated as a single value. Default: `true`|
| `default_value::Any`                          | When a certain key exists in one branch, but not another, what value should be used to fill missing. Default: `missing` |
| `pool_arrays::Bool`                           | When collecting vectors for columns, choose whether to use PooledArrays instead of Base.Vector |
| `column_names::Dict{Vector{Symbol}, Symbol}`  | Provide a mapping of key/fieldname paths to replaced column names |
| `column_style::T<:ExpandNestedData.ColumnStyle` | Chose returned column style from `nested_columns` or `flat_columns`. If nested, column_names are ignored and a TypedTables.Table is returned for which the columns are nested in the same structure as the source data. Default: `flat_columns` |

```@example
using ExpandNestedData #hide
using JSON3 #hide
using DataFrames #hide

message = Dict( :a => [ Dict(:b => 1, :c => 2), Dict(:b => 2), Dict(:b => [3, 4], :c => 1), Dict(:b => []) ], :d => 4) #hide

name_map = Dict([:a, :b] => :Column_B)
expand(message; flatten_arrays=true, default_value="no value", pool_arrays=true, column_names=name_map) |> DataFrame
```
### Using ColumnDefintions
Instead of setting the configurations for the whole dataset, you can use a
`Vector{ColumnDefinition}` to control how each column is handled. `ColumnDefinition` has the
added benefit of allowing you to ignore certain fields from the input.

```@example
using ExpandNestedData #hide
using JSON3 #hide
using DataFrames #hide

message = Dict( :a => [ Dict(:b => 1, :c => 2), Dict(:b => 2), Dict(:b => [3, 4], :c => 1), Dict(:b => []) ], :d => 4) #hide

column_defs = [
    ColumnDefinition([:d]; column_name = :ColumnD),
    ColumnDefinition([:a, :b]; flatten_arrays=true),
    ColumnDefinition([:e, :f]; column_name = :MissingColumn, default_value="Missing branch")
]

expand(message, column_defs) |> DataFrame
```
The only difference in the kwargs API here is that `column_names` is `column_name` and accepts
a single `Symbol`.

### ColumnStyles
In the examples above, we've used `flat_columns` style. However, we can also maintain the nesting heirarchy
of the source data. 
```@example
using ExpandNestedData #hide
using JSON3 #hide
using TypedTables


message = Dict( :a => [ Dict(:b => 1, :c => 2), Dict(:b => 2), Dict(:b => [3, 4], :c => 1), Dict(:b => []) ], :d => 4) #hide

tbl = expand(message; column_style = nested_columns)
tbl.a.b[1] == 1 # true
# `rows(tbl)` returns a nested NamedTuple 
tbl |> rows |> first
```

## API
```@docs
ExpandNestedData.expand(::Any)
ExpandNestedData.expand(::Any, ::Vector{ExpandNestedData.ColumnDefinition})
ExpandNestedData.ColumnDefinition
```
