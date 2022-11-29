using PooledArrays
using Test
using JSON3
using Normalize

ND = Normalize

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

@testset "Normalize" begin
    simple_test_body = JSON3.read("""
    {"data" : [
        {"E" : 7, "D" : 1},
        {"E" : 8, "D" : 2}
    ]}""")

    expected_simple_table = (data_E=[7,8], data_D=[1,2])
    @test ND.normalize(simple_test_body) == expected_simple_table

    test_body_str = """
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
    test_body = JSON3.read(test_body_str)
    
    
    actual_expanded_table = ND.normalize(test_body; flatten_arrays=true)
    @test begin
        expected_table_expanded = (
            a_b=[1,2,3,4,missing], 
            a_c=[2,missing,1,1, missing], 
            d=[4,4,4,4,4])
        fieldsequal(actual_expanded_table, expected_table_expanded)
    end
    @test eltype(actual_expanded_table.d) == Int64
    @test begin
        expected_table = (
            Column_B=[1,2,[3,4],missing], 
            a_c=[2, missing,1, missing], 
            d=[4,4,4,4])
        name_map = Dict([:a, :b] => :Column_B)
        fieldsequal(ND.normalize(test_body; flatten_arrays=false, column_names = name_map), expected_table)
    end

    struct InternalObj
        b
        c
    end
    struct MainBody
        a::Vector{InternalObj}
        d
    end
    struct_body = JSON3.read(test_body_str, MainBody)
    @test begin
        expected_table_expanded = (
            a_b=[1,2,3,4,nothing], 
            a_c=[2,nothing,1,1, nothing], 
            d=[4,4,4,4,4])
        fieldsequal(ND.normalize(struct_body; flatten_arrays=true, default_value=nothing), expected_table_expanded)
    end
    @test typeof(ND.normalize(struct_body; pool_arrays=true).d) == typeof(PooledArray([4,4,4,4,4]))

    
    columns_defs = [
        ND.ColumnDefinition([:d]),
        ND.ColumnDefinition([:a, :b]; flatten_arrays=true),
        ND.ColumnDefinition([:a, :c]),
        ND.ColumnDefinition([:e, :f]; default_value="Missing branch")
        ]
    expected_table = (d=[4,4,4,4,4], a_b=[1,2,3,4, missing], a_c=[2,missing,1,1, missing], 
        e_f = repeat(["Missing branch"], 5)
    )
    @test isequal(ND.normalize(test_body, columns_defs), expected_table)
end
