# Scenario Layout

This folder groups the non-default test mappings and configs by scenario.

## Scenarios

- `additional_layer/`
  - `mapping.ttl`
  - `config.ini`
- `join_cache_test/`
  - `mapping.ttl`
  - `config.ini`
- `flush_stress_test/`
  - `mapping.ttl`
  - `config.ini`
  - `mapping_ratings_small.ttl`
  - `mapping_ratings_full.ttl`
  - `config_ratings_small.ini`
  - `config_ratings_full.ini`
  - `config_ratings_small_benchmark_baseline.ini`
  - `config_ratings_small_benchmark_optimized.ini`
- `stress_test_100/`
  - `mapping.ttl`
  - `config.ini`
- `base_join_control/`
  - `mapping.ttl`
  - `config.ini`

## Run Commands

From the project root:

```powershell
.\.venv\Scripts\python.exe run_rdfizer.py scenarios\additional_layer\config.ini
.\.venv\Scripts\python.exe run_rdfizer.py scenarios\join_cache_test\config.ini
.\.venv\Scripts\python.exe run_rdfizer.py scenarios\flush_stress_test\config.ini
.\.venv\Scripts\python.exe run_rdfizer.py scenarios\base_join_control\config.ini
.\\.venv\\Scripts\\python.exe run_rdfizer.py scenarios\\flush_stress_test\\config_ratings_small.ini
.\\.venv\\Scripts\\python.exe run_rdfizer.py scenarios\\flush_stress_test\\config_ratings_full.ini
.\\.venv\\Scripts\\python.exe run_rdfizer.py scenarios\\flush_stress_test\\config_ratings_small_benchmark_baseline.ini
.\\.venv\\Scripts\\python.exe run_rdfizer.py scenarios\\flush_stress_test\\config_ratings_small_benchmark_optimized.ini
.\\.venv\\Scripts\\python.exe run_rdfizer.py scenarios\\stress_test_100\\config.ini
.\\.venv\\Scripts\\python.exe run_rdfizer.py scenarios\\base_join_control\\config_ratings_small.ini
.\\.venv\\Scripts\\python.exe run_rdfizer.py scenarios\\base_join_control\\config_ratings_small_level.ini
```

## Output Layout

Scenario configs now write into subfolders under `output/`:

- `output/additional_layer/`
- `output/join_cache_test/`
- `output/flush_stress_test/`
- `output/stress_test_100/`
- `output/base_join_control/`
