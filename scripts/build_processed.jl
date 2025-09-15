#!/usr/bin/env julia
using Pkg
Pkg.activate(".")
using SkiLookup

SkiLookup.build_processed("data/raw/resorts.csv", "data/raw/snow.csv"; out_dir="data/processed")
println("Fertig: data/processed/{resorts_processed.csv, snow_joined.csv, resort_index.csv}")
