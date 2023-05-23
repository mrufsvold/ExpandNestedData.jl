
# Link a list of keys into an underscore separted column name
join_names(names, joiner="_") = names .|> string |> (s -> join(s, joiner)) |> Symbol

function anys(dims)
    a = Array{Any}(undef, dims)
    fill!(a, :not_yet_initialized)
end
