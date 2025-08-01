# ExpandNestedData.jl

ExpandNestedData.jl is a small package that can consume nested data structures like dictionaries of dictionaries or structs of structs and produce a normalized, Tables.jl-compliant NamedTuple. It can be used with JSON3.jl, XMLDict.jl, and other packages that parse file formats which are structured as denormalized data. It operates similarly to [`Pandas.json_normalize`](https://pandas.pydata.org/docs/reference/api/pandas.json_normalize.html), but it is much more flexible.

## Getting Started

### Install

```julia
using Pkg
Pkg.add("ExpandNestedData")
```

### Basic Usage

`ExpandNestedData` provides a single function `expand` to flatten out nested data.

```@example 1
using ExpandNestedData # hide
using JSON3

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

expand(message)
```

## Configuring Options

While `expand` can produce a `Table` out-of-the-box, it is often useful to configure some options in how it handles the normalization process. `ExpandNestedData.jl` offers two ways to set these configurations. You can set them at the table-level with `expand`'s keyword arguments or exercise finer control with per-column configurations.

### Keyword Arguments

| Parameter | Description |
| --------- | ----------- |
| `default_value::Any` | The default value to use when a path is missing. Default: `missing` |
| `name_join_pattern::String` | The pattern to use when joining names. Default: `"_"` |
| `column_style::Symbol` | The style of the output table. Either `:flat` or `:nested`. When `:flat`, the output table will be a flat table with names joined by `name_join_pattern`. When `:nested`, the output table will be a nested table. Default: `:flat` |
| `lazy_columns::Bool` | If `true`, columns are returned as a lazy `AbstractArray`. Otherwise, columns are returned as a `Vector`. Default: `false` |
| `use_xpath_names::Bool` | If `true`, the column names will be generated using XPath notation. `name_join_pattern` will be ignored. Default: `false` |

```@example 1
expand(message; default_value="no value", ) |> DataFrame
```

### Using ColumnDefinitions

Instead of setting the configurations for the whole dataset, you can use a `Vector{ColumnDefinition}` to control how each column is handled. Using `ColumnDefinition`s has the added benefit of allowing you to ignore certain fields from the input. `ColumnDefinition` takes a `Vector` or `Tuple` of keys that act as the path to the values for the column. It also supports most of the keyword arguments as the regular `expand` API with the following exceptions:

* `column_name` accepts a single `Symbol` which will overwrite the name of the returned column if the overall table is `:flat`.
* `column_style` does not apply.
* No support for `lazy_columns` at this time.
* No support for `use_xpath_names` at this time.

```@example 1

column_defs = [
    ColumnDefinition([:d]; column_name = :ColumnD),
    ColumnDefinition([:a, :b]),
    ColumnDefinition([:e, :f]; column_name = :MissingColumn, default_value="Missing branch")
]

expand(message, column_defs) |> DataFrame
```

### ColumnStyles

In the examples above, we've used `flat_columns` style. However, we can also maintain the nesting hierarchy of the source data.

```@example 1
using TypedTables

tbl = expand(message; column_style = :nested)
```

Now, our table has its columns nested, so we can access a specific column using `dot` syntax.

```@example 1
tbl.a.b
```

Furthermore, `rows(tbl)` returns a nested NamedTuple for each row

```@example 1
tbl |> rows |> first
```

## API

```@docs
expand(::Any, ::Vector{ExpandNestedData.ColumnDefinition})
ColumnDefinition(::Any;)
```
