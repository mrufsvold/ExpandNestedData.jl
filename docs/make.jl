project_src = joinpath(dirname(@__DIR__), "src")
push!(LOAD_PATH, project_src)
using Documenter, NormalizeDict

makedocs(
    sitename="NormalizeDict",
    modules = Module[NormalizeDict],
    pages = ["Index" => "index.md"]    
    
    )
