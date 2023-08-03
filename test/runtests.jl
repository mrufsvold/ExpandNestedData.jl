using PooledArrays
using Test
using JSON3
using ExpandNestedData
import ExpandNestedData: NestedIterators, 
                        ColumnSetManagers, 
                        NameLists, 
                        PathGraph,
                        ColumnDefinitions
using TypedTables
using DataStructures: OrderedRobinDict

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

function get_rows(t, fields, len)
    return [
        Dict(
            f => t[f][i]
            for f in fields
        )
        for i in 1:len
    ]
end
function unordered_equal(t1, t2)
    fields = keys(t1)
    len = length(t1[1])
    matches = Set(get_rows(t1, fields,len)) == Set(get_rows(t2, fields,len))
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
    @testset "Internals" begin
        @testset "NestedIterators and ColumnSets" begin
            csm = ExpandNestedData.ColumnSetManager()
            iter1_2() = NestedIterators.RawNestedIterator(csm, [1,2])
            @test [1,2] == collect(iter1_2(), csm)
            @test [1,2,1,2] == collect(NestedIterators.cycle(iter1_2(), 2), csm)
            @test [1,1,2,2] == collect(NestedIterators.repeat_each(iter1_2(), 2), csm)
            @test [1,2,1,2] == collect(vcat(iter1_2(), iter1_2()), csm)
            col_set = ExpandNestedData.ColumnSet(
                NameLists.NameID(2) => NestedIterators.RawNestedIterator(csm, [3,4,5,6]),
                NameLists.NameID(1) => NestedIterators.RawNestedIterator(csm, [1,2]),
            )
            @test collect(keys(col_set)) == [NameLists.NameID(1),NameLists.NameID(2)]
            col_set2 = ExpandNestedData.ColumnSet(
                NameLists.NameID(1) => NestedIterators.RawNestedIterator(csm, [1,2,1,2]),
                NameLists.NameID(2) => NestedIterators.RawNestedIterator(csm, [3,4,5,6]),
            )
            @test isequal(ColumnSetManagers.cycle_columns_to_length!(col_set), col_set2, csm)

            # popping columns
            @test ColumnSetManagers.get_first_key(col_set) == NameLists.NameID(1)
            default_col = pop!(col_set, NameLists.NameID(3), NestedIterators.RawNestedIterator(csm, [1]))
            @test isequal(default_col, NestedIterators.RawNestedIterator(csm, [1,1,1,1]), csm)
            popped_col = pop!(col_set, NameLists.NameID(2), NestedIterators.RawNestedIterator(csm, [1]))
            @test collect(popped_col, csm) == [3,4,5,6]
            @test collect(keys(col_set)) == [NameLists.NameID(1)]

            # column length 
            @test ColumnSetManagers.get_total_length([col_set, col_set2]) == 8
            @test ColumnSetManagers.column_length(ColumnSetManagers.repeat_each_column!(col_set, 2)) == 8

            # column set manager
            csm = ExpandNestedData.ColumnSetManager()
            cs = ColumnSetManagers.get_column_set(csm)
            @test isequal(cs, ColumnSetManagers.ColumnSet(), csm)
            ColumnSetManagers.free_column_set!(csm, cs)
            @test !isempty(csm.column_sets)
            cs = ColumnSetManagers.get_column_set(csm)
            @test isempty(csm.column_sets)

            cs[NameLists.NameID(3)] = NestedIterators.RawNestedIterator()
            cs[NameLists.NameID(1)] = NestedIterators.RawNestedIterator()
            @test collect(keys(cs)) == [NameLists.NameID(1),NameLists.NameID(3)]

            name = :test_name
            id = ExpandNestedData.get_id(csm, name)
            @test id == NameLists.NameID(2)
            @test id == ExpandNestedData.get_id(csm, name)
            @test name == ExpandNestedData.get_name(csm, id)
            field_path = (name,)
            id_path = (id,)
            id_for_path = ExpandNestedData.get_id(csm, id_path)
            @test id_for_path == ExpandNestedData.get_id_for_path(csm, field_path)

            # NameLists 
            top = NameLists.NameList()
            l = NameLists.NameList(top, id)
            id_for_tuple_from_list = ExpandNestedData.get_id(csm, l)
            @test id_for_tuple_from_list == id_for_path
            @test ExpandNestedData.ColumnSetManagers.reconstruct_field_path(csm, id_for_tuple_from_list) == field_path

            # Rebuild ColumnSet
            raw_cs = ExpandNestedData.ColumnSet(id_for_path => NestedIterators.RawNestedIterator(csm, [1]))
            finalized_col = NestedIterators.NestedIterator(csm, NestedIterators.RawNestedIterator(csm, [1]))
            @test OrderedRobinDict((name,) => finalized_col) == ColumnSetManagers.build_final_column_set(csm, raw_cs)
        end

        @testset "ColumnDefinitions and PathGraph" begin
            @test fieldsequal(ColumnDefinition((:a,)), ColumnDefinition([:a]))
            coldef = ColumnDefinition((:a,:b), Dict(); pool_arrays=false, name_join_pattern = "^")
            @test coldef == ColumnDefinition((:a,:b), Symbol("a^b"), missing, false)
            @test ColumnDefinitions.current_path_name(coldef, 2) == :b
            @test collect(ColumnDefinitions.make_column_def_child_copies([coldef], :a, 1)) == [coldef]

            csm = ExpandNestedData.ColumnSetManager()
            simple = PathGraph.SimpleNode(csm, :a)
            value = PathGraph.ValueNode(csm, :a, :a, (:a,), false, NestedIterators.RawNestedIterator(csm, [1]))
            path_n = PathGraph.PathNode(csm, :a, PathGraph.Node[value])
            @test all_equal(ExpandNestedData.get_name.((simple,value,path_n)))
            for (f,result) in ((
                    PathGraph.get_final_name, NameLists.NameID(2)), 
                    (PathGraph.get_field_path,NameLists.NameID(4)), 
                    (PathGraph.get_pool_arrays,false))
                @test_throws ErrorException f(simple)
                @test_throws ErrorException f(path_n)
                @test f(value) == result
            end

            @test PathGraph.get_all_value_nodes(path_n) == [value]
            @test isequal(PathGraph.get_default(value), NestedIterators.RawNestedIterator(csm, [1]),csm)
        end

        @testset "Utils" begin
            @test ExpandNestedData.all_eltypes_are_values(Vector{Union{Int64, String, Float64}})
            @test !ExpandNestedData.all_eltypes_are_values(Vector{Union{Int64, String, AbstractFloat}})
            @test !ExpandNestedData.all_eltypes_are_values(Vector{Union{Dict, String}})
            d = Dict(:a => 1, :b => 2)
            @test ExpandNestedData.get_names(d) == keys(d)
            struct _T_
                a
            end

            @test collect(ExpandNestedData.get_names(_T_(1))) == collect(fieldnames(_T_))
            @test ExpandNestedData.get_value(d, :a, 3) == 1
            @test ExpandNestedData.get_value(d, :c, 3) == 3
            @test ExpandNestedData.get_value(_T_(1), :a, 3) == 1
            @test ExpandNestedData.join_names((:a,1,"hi"), ".") == Symbol("a.1.hi")
        end

        @testset "Core" begin
            csm = ExpandNestedData.ColumnSetManager()
            name_list = NameLists.NameList()
            node = PathGraph.SimpleNode(NameLists.NameID(0))
            col_num = 5
            dict_step = ExpandNestedData.DictStep(name_list, Dict(), node)
            array_step = ExpandNestedData.ArrayStep(name_list, [], node)
            leaf_step = ExpandNestedData.LeafStep(name_list, 1)
            default_step = ExpandNestedData.DefaultStep(name_list)
            merge_step = ExpandNestedData.MergeStep(col_num)
            stack_step = ExpandNestedData.StackStep(col_num)
            col_step = ExpandNestedData.NewColumnSetStep(ExpandNestedData.get_column_set(csm))

            # test get_name
            for s in (dict_step, array_step, leaf_step, default_step)
                @test ExpandNestedData.get_name(s) == name_list
            end
            for s in (merge_step, stack_step, col_step)
                @test_throws ErrorException ExpandNestedData.get_name(s)
            end

            # test get_data
            for (s,expected) in ((dict_step, Dict()),(array_step,[]),(leaf_step,1))
                @test ExpandNestedData.get_data(s) == expected
            end
            for s in (default_step, merge_step, stack_step, col_step)
                @test_throws ErrorException ExpandNestedData.get_data(s)
            end

            # test get_column_number
            for s in (merge_step, stack_step)
                @test ExpandNestedData.get_column_number(s) == col_num
            end
            for s in (default_step, dict_step,array_step,leaf_step, col_step)
                @test_throws ErrorException ExpandNestedData.get_column_number(s)
            end

            # test get_path_node
            for s in (dict_step,array_step)
                @test ExpandNestedData.get_path_node(s) == node
            end
            for s in (default_step, leaf_step, col_step, merge_step, stack_step)
                @test_throws ErrorException ExpandNestedData.get_path_node(s)
            end

             # test get_column_set
             for s in (col_step,)
                @test isequal(ExpandNestedData.get_column_set(s), ExpandNestedData.ColumnSet(),csm)
            end
            for s in (dict_step,array_step, default_step, leaf_step, merge_step, stack_step)
                @test_throws ErrorException ExpandNestedData.get_column_set(s)
            end

            @test isequal(
                ExpandNestedData.get_column_set(ExpandNestedData.empty_column_set_step(csm)), 
                ExpandNestedData.ColumnSet(),
                csm)
            
            @test begin 
                column_defs = [
                        ColumnDefinitions.ColumnDefinition((:data,)),
                        ColumnDefinitions.ColumnDefinition((:data, :E))
                    ]
                path_graph = PathGraph.make_path_graph(csm, column_defs)
                actual_col_set = ExpandNestedData.make_missing_column_set(csm, path_graph)
                expected_col_set = ExpandNestedData.ColumnSet(
                    ExpandNestedData.get_id_for_path(csm, (:data, NameLists.unnamed)) => NestedIterators.RawNestedIterator(csm, [missing]),
                    ExpandNestedData.get_id_for_path(csm, (:data, :E)) => NestedIterators.RawNestedIterator(csm, [missing])
                )
                isequal(actual_col_set, expected_col_set,csm)
            end
        end
    end

    @testset "DataStructure Internals" begin
        d = OrderedRobinDict(:a => 1, :b => missing)
        k = d.keys
        @test k isa Vector{Symbol}
        @test k[2] == :b
        d[:b] = 5 
        @test (d[:b]) == 5
    end


    # Source Data
    simple_test_body = JSON3.read("""
    {"data" : [
        {"E" : 7, "D" : 1},
        {"E" : 8, "D" : 2}
    ]}""")
    expected_simple_table = (data_E=[7,8], data_D=[1,2])

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
        actual_simple_table = ExpandNestedData.expand(simple_test_body)
        @test unordered_equal(actual_simple_table, expected_simple_table)
        @test eltype(actual_simple_table.data_D) == Int64

        # Expanding Arrays
        @test begin
            actual_expanded_table = ExpandNestedData.expand(test_body)
            expected_table_expanded = (
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
            output = (
                arr1 = [1,1,2,2,3,3],
                arr2 = [4,5,4,5,4,5]
            )
            unordered_equal(ExpandNestedData.expand(input), output)
        end

        # Test multiple missing columns in array
        @test begin
            input = [
                Dict(:a=>1),
                Dict(:a=>2),
                Dict(:a=>3),
                Dict(:a=>4, :b =>5),
            ]
            output = (
                a = [1,2,3,4],
                b = [missing,missing,missing,5]
            )
            unordered_equal(ExpandNestedData.expand(input), output)
        end

        # test world age problem with get index and evaled switch function
        @test begin
            function test_world_age(input)
                t = expand(input; lazy_columns=true)
                first_el = t.a_b[1]
                comprehension_collect = [el for el in t.a_b]
                return isequal(first_el, first(comprehension_collect))
            end
            test_world_age(test_body)
        end

        # Using struct of struct as input
        @test begin
            expected_table_expanded = (
                new_column=[1,2,3,4,nothing], 
                a_c=[2,nothing,1,1, nothing], 
                d=[4,4,4,4,4])
            unordered_equal(
                ExpandNestedData.expand(struct_body; default_value=nothing, column_names= Dict((:a, :b) => :new_column)), 
                expected_table_expanded)
        end
        @test (typeof(ExpandNestedData.expand(struct_body; pool_arrays=true, lazy_columns=false).d) == 
            typeof(PooledArray(Int64[])))
        
        @test fieldsequal((ExpandNestedData.expand(struct_body; column_style=:nested) |> rows |> last), (a=(b=1,c=2), d=4))

        @test unordered_equal(ExpandNestedData.expand(heterogenous_level_test_body), (data = [5], data_E = [8]))

        empty_dict_field = Dict(
            :a => Dict(),
            :b => 5
        )
        @test unordered_equal(ExpandNestedData.expand(empty_dict_field), (b = [5],))

        @test begin
            two_layer_deep = Dict(
                :a => Dict(
                    :b => Dict(
                        :c => 1,
                        :d => 2,
                    )
                )
            )
            unordered_equal(ExpandNestedData.expand(two_layer_deep), (a_b_c = [1], a_b_d = [2]))
        end
    end


    @testset "Configured Expand" begin
        columns_defs = [
            ColumnDefinitions.ColumnDefinition((:d,)),
            ColumnDefinitions.ColumnDefinition((:a, :b)),
            ColumnDefinitions.ColumnDefinition((:a, :c); name_join_pattern = "?_#"),
            ColumnDefinitions.ColumnDefinition((:e, :f); default_value="Missing branch")
            ]
        expected_table = NamedTuple((:d=>[4,4,4,4,4], :a_b=>[1,2,3,4, missing], Symbol("a?_#c")=>[2,missing,1,1, missing], 
            :e_f => repeat(["Missing branch"], 5))
        )
        @test unordered_equal(ExpandNestedData.expand(test_body, columns_defs), expected_table)
        @test fieldsequal(
            ExpandNestedData.expand(test_body, columns_defs; column_style=:nested) |> rows |> last, 
            (d=4, a=(b = 1, c = 2), e = (f="Missing branch",))
        )
        columns_defs = [
            ColumnDefinitions.ColumnDefinition((:data,)),
            ColumnDefinitions.ColumnDefinition((:data, :E))
        ]
        @test unordered_equal(ExpandNestedData.expand(heterogenous_level_test_body, columns_defs), (data = [5], data_E = [8]))

    end

    @testset "superficial options" begin
        # Expanding Arrays
        actual_expanded_table = ExpandNestedData.expand(test_body; name_join_pattern = "?_#")
        @test begin
            expected_table_expanded = NamedTuple((
                Symbol("a?_#b")=>[1,2,3,4,missing], 
                Symbol("a?_#c")=>[2,missing,1,1, missing], 
                :d=>[4,4,4,4,4]))
            unordered_equal(actual_expanded_table, expected_table_expanded)
        end
    end
end
