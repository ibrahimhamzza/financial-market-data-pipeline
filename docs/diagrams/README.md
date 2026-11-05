# Project Diagrams

Canonical, version-controlled Mermaid sources for the Phase 1 system analysis and design.
The actual implementation (`pipeline.py`, `ingestion/`, `transform/`, `database/`, and
`dags/financial_pipeline_dag.py`) is the source of truth.

## Sources

| Diagram | Mermaid source | Embedded in | Exports |
|---------|----------------|-------------|---------|
| System architecture | [`system-architecture.mmd`](./system-architecture.mmd) | §2 of [`../08_system_analysis_and_design.md`](../08_system_analysis_and_design.md) | SVG, PNG |
| DFD level 0 (context) | [`dfd-level-0.mmd`](./dfd-level-0.mmd) | §4 | SVG, PNG |
| DFD level 1 | [`dfd-level-1.mmd`](./dfd-level-1.mmd) | §4 | SVG, PNG |
| Sequence — incremental weekday run | [`sequence-diagram.mmd`](./sequence-diagram.mmd) | §5 | SVG, PNG |
| Activity — extraction and pipeline flow | [`activity-diagram.mmd`](./activity-diagram.mmd) | §6 | SVG, PNG |
| Component / function view | [`component-diagram.mmd`](./component-diagram.mmd) | §9 | SVG, PNG |
| State — `pipeline_runs.status` | [`state-diagram.mmd`](./state-diagram.mmd) | §7 | SVG, PNG |
| ERD | [`erd.mmd`](./erd.mmd) | §10 | SVG, PNG |

The same diagram sources are embedded directly in
[`docs/08_system_analysis_and_design.md`](../08_system_analysis_and_design.md). Keep the
embedded copy byte-for-byte consistent with the `.mmd` file when editing a diagram.

## Diagram conventions

- **Persistent PostgreSQL tables** (`raw_market_data`, `transformed_market_data`,
  `pipeline_runs`) use cylinder nodes and a green style.
- **Intermediate batch files** (`raw_data/*.json`, `_staging.csv`, `_metrics.csv`) use
  file/subroutine nodes and a yellow style. They are not database tables.
- `pipeline.extract_task()` owns `_staging.csv` creation/removal and the `NO_NEW_DATA`
  sentinel. `extractor.run_extraction()` only returns a DataFrame or raises
  `ExtractionError`.
- `database/loader.py` owns both the data helpers (`load_raw`, `load_transformed`,
  `fetch_raw_for_symbols`, `load_to_db`) and the pipeline audit helpers
  (`start_pipeline_run`, `finish_pipeline_run`). The audit helpers are called by
  `pipeline.run_pipeline()`.
- The Airflow DAG invokes the five task callables directly, so `pipeline_runs` audit rows
  are written only on the `run_pipeline()` CLI path.

## Regenerating exports

Exports live in [`exports/`](./exports/). The three main diagrams
(`system-architecture`, `dfd-level-1`, `erd`) must be present as both SVG and PNG; the
other diagrams are exported as well for convenience.

Using the Mermaid CLI directly:

```bash
mmdc -i system-architecture.mmd -o exports/system-architecture.svg
mmdc -i system-architecture.mmd -o exports/system-architecture.png -b white --size 3000
```

Using Docker (no local Node.js install required):

```bash
./export.sh
```

The export script uses `mmdc` when available, otherwise Docker. Set `MERMAID_IMAGE` to
override the container image (default: `minlag/mermaid-cli:latest`).
