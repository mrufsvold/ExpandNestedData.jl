using Documenter, Normalize

makedocs(
    sitename="Normalize.jl",
    modules = Module[Normalize],
    pages = ["Contents" => "index.md"]    
)

deploydocs(
    repo = "github.com/mrufsvold/Normalize.jl.git",
    devbranch = "main"
)
