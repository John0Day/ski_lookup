#!/usr/bin/env julia
# CLI – starten mit:
#   julia --project=. bin/ski_lookup.jl --list
#   julia --project=. bin/ski_lookup.jl --name "Zermatt"

try
    using Pkg
    Pkg.activate(joinpath(@__DIR__, ".."))
catch e
    @warn "Projekt nicht aktiviert: $e"
end

using SkiLookup, ArgParse, CSV

function parser()
    s = ArgParseSettings()
    @add_arg_table! s begin
        "--list"
            help = "Liste der Skigebiete anzeigen"
            action = :store_true
        "--name","-n"
            help = "Exakter Name des Resorts (wie in --list)"
        "--data-dir","-d"
            help = "Verzeichnis mit processed-Dateien"
            default = "data/processed"
        "--save"
            help = "Zeitreihe als CSV exportieren (Pfad)"
        "--maxrows","-m"
            help = "Zeilen in der Anzeige"
            arg_type = Int
            default = 20
    end
    return s
end

function check_processed!(data_dir::AbstractString)
    missing = String[]
    for f in ("resorts_processed.csv", "snow_joined.csv")
        path = joinpath(data_dir, f)
        isfile(path) || push!(missing, path)
    end
    if !isempty(missing)
        @error "Processed-Dateien fehlen:\n  * $(join(missing, "\n  * "))\n→ Bitte zuerst: `julia --project=. scripts/build_processed.jl`"
        exit(2)
    end
end

function main()
    args = parse_args(parser())
    data_dir = args["data-dir"]

    if get(args, "list", false)
        check_processed!(data_dir)
        for r in SkiLookup.list_resorts(data_dir)
            println(r)
        end
        return
    end

    if !haskey(args, "name") || isnothing(args["name"])
        @error "Bitte erst --list und dann mit --name \"GENAUER NAME\" aufrufen."
        exit(3)
    end

    check_processed!(data_dir)
    res = SkiLookup.query_resort(args["name"]; data_dir=data_dir)
    SkiLookup.print_resort_report(res; maxrows=args["maxrows"])

    if haskey(args, "save") && !isnothing(args["save"])
        CSV.write(args["save"], res.series)
        println("\nExportiert nach ", args["save"])
    end
end

abspath(PROGRAM_FILE) == @__FILE__ && main()
