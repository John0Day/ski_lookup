module SkiLookup

using CSV, DataFrames, Dates, PrettyTables, Statistics

# ============ Helpers ============
normalize_str(x) = lowercase(strip(String(x)))

const R_EARTH_KM = 6371.0
deg2rad(x) = x*pi/180
function haversine_km(lat1, lon1, lat2, lon2)
    φ1, λ1, φ2, λ2 = deg2rad.(lat1), deg2rad.(lon1), deg2rad.(lat2), deg2rad.(lon2)
    dφ, dλ = φ2-φ1, λ2-λ1
    a = sin(dφ/2)^2 + cos(φ1)*cos(φ2)*sin(dλ/2)^2
    2R_EARTH_KM * asin(min(1, sqrt(a)))
end

# ============ Loader (raw CSV) ============
"Resorts: erwartet mind. Spalten: Resort, Latitude, Longitude"
function load_resorts_csv(path::AbstractString)
    df = CSV.read(path, DataFrame)

    # Pflichtspalten korrekt prüfen (kein gefährliches Broadcasting)
    required = ["Resort","Latitude","Longitude"]
    @assert all(x -> x in names(df), required) "resorts.csv braucht Spalten: Resort, Latitude, Longitude"

    # Normalisierter Name + stabile ID
    df.name_key = normalize_str.(df.Resort)
    if :ID ∉ names(df)
        df.ID = [Int(abs(hash(n)) % (10^9)) for n in df.name_key]   # elementweise
        # Alternative: df.ID = Int.(abs.(hash.(df.name_key)) .% (10^9))
    end
    return df
end

"Snow: erwartet Spalten: Month (yyyy-mm-01), Latitude, Longitude, Snow"
function load_snow_csv(path::AbstractString)
    df = CSV.read(path, DataFrame)
    @assert all(x -> x in names(df), ["Month","Latitude","Longitude","Snow"]) "snow.csv braucht Month, Latitude, Longitude, Snow"

    # Datum robust parsen
    df.date = tryparse.(Date, String.(df.Month), Ref(dateformat"y-m-d"))
    @assert all(.!ismissing.(df.date)) "Month konnte nicht als Datum geparst werden (erwartet yyyy-mm-dd)"

    select!(df, [:date, :Latitude, :Longitude, :Snow])
    rename!(df, [:date, :lat, :lon, :snow_cm])
    return df
end

# ============ ETL / Join ============
"""
build_processed(resorts_csv, snow_csv; out_dir="data/processed")

- ordnet jedem Resort den nächstgelegenen Snow-Gridpunkt zu
- schreibt CSV: resorts_processed.csv, snow_joined.csv, resort_index.csv
"""
function build_processed(resorts_csv::AbstractString, snow_csv::AbstractString; out_dir::AbstractString="data/processed")
    resorts = load_resorts_csv(resorts_csv)
    snow    = load_snow_csv(snow_csv)

    grid = unique(select(snow, [:lat, :lon]))
    @assert nrow(resorts) > 0 "resorts.csv ist leer?"
    @assert nrow(grid)    > 0 "snow.csv ist leer?"

    # Vektoren mit richtiger Länge anlegen (kein similar(...) ohne Länge!)
    nearest_lat = Vector{Float64}(undef, nrow(resorts))
    nearest_lon = Vector{Float64}(undef, nrow(resorts))

    # simple NN-Suche via Haversine
    for (i, r) in enumerate(eachrow(resorts))
        dmin, jmin = Inf, 0
        for (j, g) in enumerate(eachrow(grid))
            d = haversine_km(r.Latitude, r.Longitude, g.lat, g.lon)
            if d < dmin
                dmin, jmin = d, j
            end
        end
        nearest_lat[i] = grid.lat[jmin]
        nearest_lon[i] = grid.lon[jmin]
    end
    resorts.grid_lat = nearest_lat
    resorts.grid_lon = nearest_lon

    joined = leftjoin(
        snow,
        select(resorts, [:ID, :Resort, :name_key, :Latitude, :Longitude, :grid_lat, :grid_lon]),
        on = [:lat=>:grid_lat, :lon=>:grid_lon]
    )

    sort!(joined, [:ID, :date])
    sort!(resorts, :Resort)

    mkpath(out_dir)
    CSV.write(joinpath(out_dir, "resorts_processed.csv"), resorts)
    CSV.write(joinpath(out_dir, "snow_joined.csv"), joined)
    CSV.write(joinpath(out_dir, "resort_index.csv"),
              select(resorts, [:ID,:Resort,:Latitude,:Longitude,:grid_lat,:grid_lon]))

    return (; resorts, snow_joined=joined)
end

# ============ Query & Ausgabe (reads processed CSV) ============
"Liste verfügbarer Gebiete"
function list_resorts(data_dir::AbstractString="data/processed")
    resorts = CSV.read(joinpath(data_dir, "resorts_processed.csv"), DataFrame)
    collect(resorts.Resort)
end

"Abfrage eines Gebietes (exakter Name wie in list_resorts)"
function query_resort(name::AbstractString; data_dir::AbstractString="data/processed")
    resorts = CSV.read(joinpath(data_dir, "resorts_processed.csv"), DataFrame)
    snow    = CSV.read(joinpath(data_dir, "snow_joined.csv"), DataFrame)

    key = normalize_str(name)
    row = findfirst(resorts.name_key .== key)
    row === nothing && error("Kein Resort gefunden: $name. Erst --list ausführen und exakten Namen verwenden.")
    rid = resorts.ID[row]

    meta = resorts[row:row, :]
    series = snow[snow.ID .== rid, :]
    sort!(series, :date)

    if nrow(series) > 0
        lastday  = maximum(series.date)
        lastsnow = first(series[series.date .== lastday, :snow_cm], 1)[1]
        m7  = mean(skipmissing(series[max(1, end-6):end, :snow_cm]))
        m30 = mean(skipmissing(series[max(1, end-29):end, :snow_cm]))
        summary = Dict("last_date"=>string(lastday), "last_snow_cm"=>lastsnow,
                       "mean_7d"=>m7, "mean_30d"=>m30, "n_days"=>nrow(series))
    else
        summary = Dict("message"=>"Keine Snow-Daten gefunden.")
    end
    return (; meta, series, summary)
end

"Schöne Konsolen-Ausgabe"
function print_resort_report(result; maxrows::Int=20)
    meta, series, s = result.meta, result.series, result.summary
    println("=== ", meta.Resort[1], " (ID ", meta.ID[1], ") ===")
    println("Koord.: (", meta.Latitude[1], ", ", meta.Longitude[1], ")  → Grid: (", meta.grid_lat[1], ", ", meta.grid_lon[1], ")")
    println("\n--- Zusammenfassung ---")
    for (k,v) in s
        println(rpad(k, 12), ": ", v)
    end
    println("\n--- Schneeserie (Top $(min(maxrows, nrow(series)))/$(nrow(series))) ---")
    if nrow(series) == 0
        println("Keine Daten.")
    else
        PrettyTables.pretty_table(first(select(series, [:date, :snow_cm]), maxrows))
        if nrow(series) > maxrows
            println("… (mehr Zeilen vorhanden)")
        end
    end
end

end # module
