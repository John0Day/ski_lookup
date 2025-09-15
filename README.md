# SkiLookup

A simple Julia project for exploring ski resort information from `resorts.csv`.

---

## 📦 Setup

Clone this repository and open it in VS Code or your terminal.

Install Julia packages (once, inside the project folder):

```bash
julia --project=. -e 'import Pkg; Pkg.activate("."); Pkg.instantiate()'

Build processed data
This creates data/processed/resorts_processed.csv from your raw file:

julia --project=. scripts/build_resorts.jl

List available resorts
julia --project=. bin/ski_lookup.jl --list

Query one resort
julia --project=. bin/ski_lookup.jl --name "NAME_OF_RESORT_FROM_LIST"
