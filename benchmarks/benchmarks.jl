using ExpandNestedData
using TypedTables

small_dict = Dict(
    :a => 1,
    :b => "2",
    :c => Dict(:e => Symbol(3), :f => 4)
)

expand(small_dict; lazy_columns=true, column_style=:nested);

many_records = [
    small_dict
    for _ in 1:100
]

@btime expand(many_records; lazy_columns=true, column_style=:nested);
 

function make_deep_dict(depth=1)
    if depth == 1
        return Dict(Symbol(depth) => 1)
    end
    return Dict(
        Symbol(i) => make_deep_dict(depth-1)
        for i in 1:3
    )
end

deep_dict = make_deep_dict(10)
@btime expand(deep_dict; lazy_columns=true, column_style=:nested);
