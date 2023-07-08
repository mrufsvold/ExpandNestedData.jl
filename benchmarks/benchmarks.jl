using ExpandNestedData

small_dict = Dict(
    :a => 1,
    :b => "2",
    :c => Dict(:e => Symbol(3), :f => 4)
)


many_records = [
    small_dict
    for _ in 1:10000
]

function make_deep_dict(depth=1)
    if depth == 1
        return Dict(Symbol(depth) => 1)
    end
    return Dict(
        Symbol(i) => make_deep_dict(depth-1)
        for i in 1:3
    )
end
deep_dict = make_deep_dict(7)
                
# expand(small_dict; lazy_columns=true, column_style=:nested)
# @btime expand($small_dict; lazy_columns=true, column_style=:nested)
# @profview ExpandNestedData.expand(small_dict; lazy_columns=true, column_style=:nested);

# expand(deep_dict; lazy_columns=true, column_style=:nested);
# @btime expand($deep_dict; lazy_columns=true, column_style=:nested);
# @profview_allocs ExpandNestedData.expand(deep_dict; lazy_columns=true, column_style=:nested) sample_rate=0.01

# expand(many_records; lazy_columns=true, column_style=:nested);
# @btime expand($many_records; lazy_columns=true, column_style=:nested);
# @descend expand(many_records; lazy_columns=true, column_style=:nested)
# @profview ExpandNestedData.expand(many_records; lazy_columns=true, column_style=:nested)
# @profview_allocs ExpandNestedData.expand(many_records; lazy_columns=true, column_style=:nested)