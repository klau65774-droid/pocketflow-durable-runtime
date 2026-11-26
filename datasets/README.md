# Sealed External Blind Datasets

All external evaluation datasets are stored **outside the repository** and referenced via the `SEALED_BENCHMARK_DIR` environment variable to keep evaluation blind and uncontaminated.

## Dataset Inventory & Licenses

| Tier | Dataset | Official Source | License | Evaluated Subset |
| :--- | :--- | :--- | :--- | :--- |
| **Tier 1A** | **tau-bench** (Retail & Airline) | https://github.com/sierra-research/tau-bench | **MIT** | `envs/retail/tasks_test.py` + `data/*.json`; `envs/airline/tasks_test.py` + `data/*.json` |
| **Tier 1B** | **BFCL v4** (Gorilla Function Calling) | https://github.com/ShishirPatil/gorilla | **Apache-2.0** | `BFCL_v4_multi_turn_{base,miss_func,miss_param}.json` & `BFCL_v4_{parallel,live_parallel_multiple}.json` + `possible_answer/` |
| **Tier 2** | **AgentDojo v1** + Chaos Suite | https://github.com/ethz-spylab/agentdojo | **MIT** | `default_suites/v1/{banking,slack,travel,workspace}/{user_tasks,injection_tasks}.py` |
| **Tier 3** | **Synthetic Redaction & DoS Suite** | Generated in-memory at runtime | **MIT** | 15 synthetic secrets vs. 18 benign decoys + fan-out/cycle-ref/byte-cap stress tests |

## Reproduction Instructions

1. Download the datasets above into an external folder outside this repository and set `SEALED_BENCHMARK_DIR`:

```powershell
$env:SEALED_BENCHMARK_DIR = "<path-to-external-sealed-benchmarks>"
.venv\Scripts\python.exe benchmarks\run_sealed_benchmarks.py --dataset-dir $env:SEALED_BENCHMARK_DIR --output docs\test_metrics.json
```

2. A minimal synthetic smoke fixture (< 5 KB) is kept in `datasets/sample/smoke_tasks.json` for fast offline CI smoke checks and is strictly disjoint from the sealed blind benchmarks.
