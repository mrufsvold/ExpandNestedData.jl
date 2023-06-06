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
Pkg.add("ExpandNestedData")
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
While `expand` can produce a `Table` out-of-the-box, it is often useful to configure
some options in how it handles the normalization process. `ExpandNestedData.jl` offers two ways to set
these configurations. You can set them at the table-level with `kwargs` to `expand` or exercise finer control with
per-column configurations.
### Keyword Arguments
| Parameter | Description |
| --------- | ----------- |
| `default_value::Any`                          | When a certain key exists in one branch, but not another, what value should be used to fill missing. Default: `missing` |
| `lazy_columns::Bool` | If true, return columns as a custom lazy iterator instead of collecting them as materialized vectors. This option can speed things up if you only need to access a subset of rows once. It is usually better to materialize the columns since `getindex()` on the lazy columns is expensive. Default: `false` |
| `pool_arrays::Bool`                           | When collecting vectors for columns, choose whether to use PooledArrays instead of Base.Vector |
| ` column_names::Dict{Tuple, Symbol}`  | Provide a mapping of key/fieldname paths to replaced column names |
| `column_style::Symbol` | Choose returned column style from `:nested` or `:flat`. If nested, `column_names` are ignored and a TypedTables.Table is returned in which the columns are nested in the same structure as the source data. Default: `:flat` |
| `name_join_pattern::String` | A pattern to put between the keys when joining the path into a column name. Default: `"_"`. |

```@example
using ExpandNestedData #hide
using JSON3 #hide
using DataFrames #hide

message = Dict( :a => [ Dict(:b => 1, :c => 2), Dict(:b => 2), Dict(:b => [3, 4], :c => 1), Dict(:b => []) ], :d => 4) #hide

name_map = Dict([:a, :b] => :Column_B)
expand(message; default_value="no value", pool_arrays=true, column_names=name_map) |> DataFrame
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
    ColumnDefinition([:a, :b]),
    ColumnDefinition([:e, :f]; column_name = :MissingColumn, default_value="Missing branch")
]

expand(message, column_defs) |> DataFrame
```
The only difference in the kwargs API here is that `column_names` is `column_name` and accepts
a single `Symbol`.

### ColumnStyles
In the examples above, we've used `flat_columns` style. However, we can also maintain the nesting hierarchy
of the source data. 
```@example
using ExpandNestedData #hide
using JSON3 #hide
message = Dict( :a => [ Dict(:b => 1, :c => 2), Dict(:b => 2), Dict(:b => [3, 4], :c => 1), Dict(:b => []) ], :d => 4) #hide
using TypedTables

tbl = expand(message; column_style = nested_columns)
tbl.a.b[1] == 1 # true
# `rows(tbl)` returns a nested NamedTuple 
tbl |> rows |> first
```

## API
```@docs
ExpandNestedData.expand(::Any)
ExpandNestedData.expand(::Any, ::Vector{ExpandNestedData.ColumnDefinition})
ExpandNestedData.ColumnDefinition(::Any;)
```
