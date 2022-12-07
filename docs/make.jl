using Documenter
using ExpandNestedData

makedocs(
    sitename="ExpandNestedData.jl",
    modules = Module[ExpandNestedData],
    pages = ["Home" => "index.md"]    
)

deploydocs(
    repo = "github.com/mrufsvold/ExpandNestedData.jl.git",
    devbranch = "main"
)
