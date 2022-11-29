project_src = joinpath(dirname(@__DIR__), "src")
push!(LOAD_PATH, project_src)
using Documenter, Normalize

makedocs(
    sitename="NormalizeDict",
    modules = Module[Normalize],
    pages = ["Index" => "index.md"]    
    
    )
