using PooledArrays
using Test
using JSON3
using ExpandNestedData
using TypedTables

EN = ExpandNestedData

fieldequal(v1, v2) = (v1==v2) isa Bool ? v1==v2 : false
fieldequal(::Nothing, ::Nothing) = true
fieldequal(::Missing, ::Missing) = true
fieldequal(a1::AbstractArray, a2::AbstractArray) = length(a1) == length(a2) && fieldequal.(a1,a2) |> all
function fieldsequal(o1, o2)
    for name in fieldnames(typeof(o1))
        prop1 = getproperty(o1, name)
        prop2 = getproperty(o2, name)
        if !fieldequal(prop1, prop2)
            println("Didn't match on $name. Got $prop1 and $prop2")
            return false
        end
    end
    return true
end


# Source Data
const simple_test_body = JSON3.read("""
{"data" : [
    {"E" : 7, "D" : 1},
    {"E" : 8, "D" : 2}
]}""")
const expected_simple_table = (data_E=[7,8], data_D=[1,2])

const test_body_str = """
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
const test_body = JSON3.read(test_body_str)

struct InternalObj
    b
    c
end
struct MainBody
    a::Vector{InternalObj}
    d
end
const struct_body = JSON3.read(test_body_str, MainBody)

@testset "Unguided Expand" begin
    actual_simple_table = EN.expand(simple_test_body)
    @test fieldsequal(actual_simple_table, expected_simple_table)
    @test eltype(actual_simple_table.data_D) == Int64

    # Expanding Arrays
    actual_expanded_table = EN.expand(test_body; flatten_arrays=true)
    @test begin
        expected_table_expanded = (
            a_b=[1,2,3,4,missing], 
            a_c=[2,missing,1,1, missing], 
            d=[4,4,4,4,4])
        fieldsequal(actual_expanded_table, expected_table_expanded)
    end

    # Unexpanded Arrays
    @test begin
        expected_table = (
            Column_B=[1,2,[3,4],[]], 
            a_c=[2, missing,1, missing], 
            d=[4,4,4,4])
        name_map = Dict([:a, :b] => :Column_B)
        fieldsequal(
            EN.expand(test_body; flatten_arrays=false, column_names = name_map), 
            expected_table)
    end

    # Using struct of struct as input
    @test begin
        expected_table_expanded = (
            a_b=[1,2,3,4,nothing], 
            a_c=[2,nothing,1,1, nothing], 
            d=[4,4,4,4,4])
        fieldsequal(
            EN.expand(struct_body; flatten_arrays=true, default_value=nothing), 
            expected_table_expanded)
    end
    @test (typeof(EN.expand(struct_body; pool_arrays=true, lazy_columns=false).d) == 
        typeof(PooledArray(Int64[])))
    
    @test fieldsequal(
        EN.expand(struct_body; column_style=EN.nested_columns) |> rows |> first,
        (a=(b=1,c=2), d=4)
    )

    heterogenous_level_test_body = Dict(
        :data => [
            Dict(:E => 8),
            5
            ]
        )
    # TODO consider returning a column that still contians nested data when we hit A
    # heterogenous array. Which would return something like:
    # expected_het_test = (data=[Dict(:E => 8),5],)
    @test_throws ArgumentError EN.expand(heterogenous_level_test_body)

    empty_dict_field = Dict(
        :a => Dict(),
        :b => 5
    )
    @test fieldsequal(EN.expand(empty_dict_field), (b = [5],))
end


@testset "Configured Expand" begin
    columns_defs = [
        EN.ColumnDefinition([:d]),
        EN.ColumnDefinition([:a, :b]; flatten_arrays=true),
        EN.ColumnDefinition([:a, :c]),
        EN.ColumnDefinition([:e, :f]; default_value="Missing branch")
        ]
    expected_table = (d=[4,4,4,4,4], a_b=[1,2,3,4, missing], a_c=[2,missing,1,1, missing], 
        e_f = repeat(["Missing branch"], 5)
    )
    @test fieldsequal(EN.expand(test_body, columns_defs), expected_table)
    @test fieldsequal(
        EN.expand(test_body, columns_defs; column_style=EN.nested_columns) |> rows |> first, 
        (d=4, a=(b = 1, c = 2), e = (f="Missing branch",))
    )
end
