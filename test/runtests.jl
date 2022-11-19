using Test, JSON3
using NormalizeDict
using StructTypes

const ND = NormalizeDict

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

@testset "Normalize Dict" begin
    simple_test_body = JSON3.read("""
    {"data" : [
        {"E" : 7, "D" : 1},
        {"E" : 8, "D" : 2}
    ]}""")

    expected_simple_table = (data_E=[7,8], data_D=[1,2])
    @test ND.normalize(simple_test_body) == expected_simple_table

    test_body = JSON3.read("""
    {
        "a" : [
            {"b" : 1, "c" : 2},
            {"b" : 2},
            {"b" : [3, 4], "c" : 1},
            {"b" : []}
        ],
        "d" : 4
    }
    """)
    
    @test begin
        expected_table = (
            a_b=[1,2,3,4,missing], 
            a_c=[2,missing,1,1, missing], 
            d=[4,4,4,4,4])
        fieldsequal(ND.normalize(test_body; expand_arrays=true), expected_table)
    end
    @test eltype(ND.normalize(test_body).d) == Int64
    @test begin
        expected_table = (
            a_b=[1,2,[3,4],missing], 
            a_c=[2, missing,1, missing], 
            d=[4,4,4,4])
        fieldsequal(ND.normalize(test_body; expand_arrays=false), expected_table)
    end
    
    
    


#     simple_columns_defs = [
#         NormalizeDict.ColumnDefinition([:data, :E]),
#         NormalizeDict.ColumnDefinition([:data, :D])]

#     expected_simple_table = (data_E=[7,8] , data_D=[1,2])
    
#     @test isequal(NormalizeDict.normalize(simple_test_body, simple_columns_defs), expected_simple_table)

#     test_body = JSON3.read("""
#     {
#         "a" : [
#             {"b" : 1, "c" : 2},
#             {"b" : 2},
#             {"b" : [3, 4], "c" : 1},
#             {"b" : []}
#         ],
#         "d" : 4
#     }
#     """)
#     columns_defs = [
#         NormalizeDict.ColumnDefinition([:d]),
#         NormalizeDict.ColumnDefinition([:a, :b]; expand_array=true),
#         NormalizeDict.ColumnDefinition([:a, :c]),
#         NormalizeDict.ColumnDefinition([:e, :f]; value_if_missing="Missing branch")
#         ]
#     expected_table = (d=[4,4,4,4,4], a_b=[1,2,3,4, missing], a_c=[2,missing,1,1, missing], 
#         e_f = repeat(["Missing branch"], 5)
#     )
#     @test isequal(NormalizeDict.normalize(test_body, columns_defs), expected_table)

#     simple_array_body = JSON3.read("""
#     [{"data" : [
#         {"E" : 7, "D" : 1},
#         {"E" : 8, "D" : 2}
#     ]},
#     {"data" : [
#         {"E" : 7, "D" : 1},
#         {"E" : 8, "D" : 2}
#     ]}
#     ]
#     """)
#     expected_simple_array_table = (data_E=[7,8,7,8] , data_D=[1,2,1,2])
#     @test isequal(NormalizeDict.normalize(simple_array_body, simple_columns_defs), expected_simple_array_table)
#     @test isequal(NormalizeDict.normalize(simple_array_body), expected_simple_array_table)

end
