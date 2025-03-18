using Test
using JSON3
using ExpandNestedData
using TypedTables
using Tables

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
function fieldsequal(o1::NamedTuple, o2::NamedTuple)
    for name in keys(o1)
        prop1 = getindex(o1, name)
        prop2 = getindex(o2, name)

        if prop1 isa NamedTuple && prop2 isa NamedTuple
            return fieldsequal(prop1, prop2)
        end
        if !fieldequal(prop1, prop2)
            println("Didn't match on $name. Got $prop1 and $prop2")
            return false
        end
    end
    return true
end


function unordered_equal(t1, t2)
    fields = propertynames(t1)
    len = length(t1[1])
    t2 = getproperties(t2, fields)
    matches = Set(Tables.rows(t1)) == Set(Tables.rows(t2))
    if !matches
        @show t1 t2
    end
    return matches
end

function all_equal(arr)
    if length(arr) == 1
        return true
    elseif length(arr) == 2
        return @inbounds isequal(arr[1], arr[2])
    end

    matches = true
    el = arr[1]
    @inbounds for i in 2:length(arr)
        matches = isequal(el, arr[i]) && matches
        el = arr[i]
    end
    return matches
end


@testset "ExpandNestedData" begin
    # Source Data
    simple_test_body = JSON3.read("""
    {"data" : [
        {"E" : 7, "D" : 1},
        {"E" : 8, "D" : 2}
    ]}""")
    expected_simple_table = FlexTable(data_E=[7,8], data_D=[1,2])

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

    homogenous_test_body = JSON3.read("""
    {
        "a" : [
            {"b" : [1], "c" : 2},
            {"b" : [2]},
            {"b" : [3, 4], "c" : 1},
            {"b" : []}
        ],
        "d" : 4
    }
    """)
    test_body = JSON3.read(test_body_str)

    struct InternalObj
        b
        c
    end
    struct MainBody
        a::Vector{InternalObj}
        d
    end
    struct_body = JSON3.read(test_body_str, MainBody)

    heterogenous_level_test_body = Dict(
            :data => [
                Dict(:E => 8),
                5
                ]
            )

    @testset "Unguided Expand" begin
        actual_simple_table = ExpandNestedData.expand(simple_test_body;)
        @test unordered_equal(actual_simple_table, expected_simple_table)
        @test eltype(actual_simple_table.data_D) == Int64

        # Expanding Arrays
        @test begin
            actual_expanded_table = ExpandNestedData.expand(test_body;)
            expected_table_expanded = FlexTable(
                a_b=[1,2,3,4,missing],
                a_c=[2,missing,1,1, missing],
                d=[4,4,4,4,4])
            unordered_equal(actual_expanded_table, expected_table_expanded)
        end

        # Test mismatched Array length
        @test begin
            input = Dict(
                :arr1 => [1,2,3],
                :arr2 => [4,5]
            )
            output = FlexTable(
                arr1 = [1,1,2,2,3,3],
                arr2 = [4,5,4,5,4,5]
            )
            unordered_equal(ExpandNestedData.expand(input;), output)
        end

        # Test multiple missing columns in array
        @test begin
            input = [
                Dict(:a=>1),
                Dict(:a=>2),
                Dict(:a=>3),
                Dict(:a=>4, :b =>5),
            ]
            output = FlexTable(
                a = [1,2,3,4],
                b = [missing,missing,missing,5]
            )
            unordered_equal(ExpandNestedData.expand(input;), output)
        end

        # Using struct of struct as input
        @test begin
            expected_table_expanded = FlexTable(
                new_column=[1,2,3,4,nothing],
                a_c=[2,nothing,1,1, nothing],
                d=[4,4,4,4,4])
            unordered_equal(
                ExpandNestedData.expand(struct_body; default_value=nothing, column_names= Dict((:a, :b) => :new_column),),
                expected_table_expanded)
        end

        @test fieldsequal(
            (ExpandNestedData.expand(struct_body; column_style=:nested,) |> rows |> first),
            (a=(b=1,c=2), d=4)
            )

        @test unordered_equal(
            ExpandNestedData.expand(heterogenous_level_test_body;),
            FlexTable(data = [5], data_E = [8])
            )

        empty_dict_field = Dict(
            :a => Dict(),
            :b => 5
        )
        @test unordered_equal(ExpandNestedData.expand(empty_dict_field;), FlexTable(b = [5],))

        @test begin
            two_layer_deep = Dict(
                :a => Dict(
                    :b => Dict(
                        :c => 1,
                        :d => 2,
                    )
                )
            )
            unordered_equal(ExpandNestedData.expand(two_layer_deep;), FlexTable(a_b_c = [1], a_b_d = [2]))
        end
        @test unordered_equal(
            ExpandNestedData.expand(homogenous_test_body; use_xpath_names=true),
            FlexTable(
                var"a[*]/b[*]" = Union{Missing, Int64}[1, 2, 3, 4, missing],
                var"a[*]/c" = Union{Missing, Int64}[2, missing, 1, 1, missing],
                d = [4, 4, 4, 4, 4]
            )
            )
    end


    @testset "Configured Expand" begin
        columns_defs = [
            ExpandNestedData.ColumnDefinition((:d,)),
            ExpandNestedData.ColumnDefinition((:a, :b)),
            ExpandNestedData.ColumnDefinition((:a, :c); name_join_pattern = "?_#"),
            ExpandNestedData.ColumnDefinition((:e, :f); default_value="Missing branch")
            ]
        expected_table = FlexTable(
            d=[4,4,4,4,4],
            a_b=[1,2,3,4, missing],
            var"a?_#c"=[2,missing,1,1, missing],
            e_f = repeat(["Missing branch"], 5)
            )
        @test fieldsequal(
            ExpandNestedData.expand(test_body, columns_defs; column_style=:nested,) |> rows |> first,
            (d=4, a=(b = 1, c = 2), e = (f="Missing branch",))
        )
        # columns_defs = [
        #     ExpandNestedData.ColumnDefinition((:data,)),
        #     ExpandNestedData.ColumnDefinition((:data, :E))
        # ]
        # @test unordered_equal(ExpandNestedData.expand(heterogenous_level_test_body, columns_defs;), (data = [5], data_E = [8]))
    end

    @testset "superficial options" begin
        # Expanding Arrays
        actual_expanded_table = ExpandNestedData.expand(test_body; name_join_pattern = "?_#",)
        @test begin
            expected_table_expanded = FlexTable(
                var"a?_#b"=[1,2,3,4,missing],
                var"a?_#c"=[2,missing,1,1, missing],
                d=[4,4,4,4,4]
                )
            unordered_equal(actual_expanded_table, expected_table_expanded)
        end
    end
end
