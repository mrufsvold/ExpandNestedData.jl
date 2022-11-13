using Test, Tables, JSON3, NormalizeDict

@testset "NormalizeJSON" begin
    simple_test_body = JSON3.read("""
    {"data" : [
        {"E" : 7, "D" : 1},
        {"E" : 8, "D" : 2}
    ]}""")

    simple_columns_defs = [
        NormalizeDict.ColumnDefinition([:data, :E]),
        NormalizeDict.ColumnDefinition([:data, :D])]

    simple_graph = NormalizeDict.make_path_graph(simple_test_body, simple_columns_defs)
    @test isequal(simple_graph.length, 2)
    expected_simple_table = (data_E=[7,8] , data_D=[1,2])
    
    @test isequal(NormalizeDict.normalize(simple_test_body, simple_columns_defs), expected_simple_table)

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
    columns_defs = [
        NormalizeDict.ColumnDefinition([:a, :b]; expand_array=true),
        NormalizeDict.ColumnDefinition([:a, :c]),
        NormalizeDict.ColumnDefinition([:d])
        ]
    expected_table = (a_b=[1,2,3,4, missing], a_c=[2,missing,1,1, missing], d=[4,4,4,4,4])
    @test isequal(NormalizeDict.normalize(test_body, columns_defs), expected_table)

    simple_test_body = """
    [{"data" : [
        {"E" : 7, "D" : 1},
        {"E" : 8, "D" : 2}
    ]},
    {"data" : [
        {"E" : 7, "D" : 1},
        {"E" : 8, "D" : 2}
    ]}
    ]
    """
    expected_simple_table = (data_E=[7,8,7,8] , data_D=[1,2,1,2])
    @test isequal(NormalizeDict.normalize(test_body, columns_defs), expected_table)

end
