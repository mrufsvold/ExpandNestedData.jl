using Documenter, Normalize

makedocs(
    sitename="Normalize.jl",
    modules = Module[Normalize],
    pages = ["Home" => "index.md"]    
)

deploydocs(
    repo = "github.com/mrufsvold/Normalize.jl.git",
    devbranch = "main"
)
