# Spark Optimization Experiments

This directory contains a comprehensive A/B testing framework for evaluating different Spark optimization strategies on implemented data processing pipelines. The experiments focus on measuring the impact of various optimizations on transformation performance in a medallion architecture (Bronze -> Silver -> Gold layers).

## Overview

The experiments compare 8 different optimization configurations:

1. **Baseline** - No optimizations (Spark defaults)
2. **Shuffle Only** - Only shuffle partitions tuned to 8
3. **AQE Only** - Only Adaptive Query Execution enabled
4. **Caching Only** - Only DataFrame caching enabled
5. **Compression Only** - Only Snappy compression enabled
6. **Broadcast Only** - Only auto-broadcast joins enabled
7. **Partitioning Only** - Only partitioning products by category
8. **All Optimizations** - All optimizations enabled together

### Key Measurement Approach

**Important:** The experiments measure **transformation time only**, excluding spark session creation, data loading, and saving I/O operations. This provides a more accurate comparison of optimization effectiveness by isolating the actual processing work from disk operations.

## Directory Structure

```
experiments/
├── config/                               # Experiment configuration files
│   ├── optimization_config.yaml          # Configuration for all experiments
│   └── optimization_experiment_config.py # Config loader class
├── process_silver_layer_experiment.py    # Silver layer with configurable opts
├── process_gold_layer_experiment.py      # Gold layer with configurable opts
├── run_experiments.py                    # Main experiment runner
├── visualize_results.py                  # Results visualization script
├── data/                                 # Experiment data (local only)
│   ├── bronze_layer/                     # Generated test data
│   ├── silver_layer/                     # Processed data per experiment
│   └── gold_layer/                       # Final results per experiment
└── results/                              # Experiment results and charts
    ├── experiment_results_*.json         # Raw results data
    ├── comparison_chart.png              # Overall comparison
    ├── speedup_chart.png                 # Speedup vs baseline
    ├── variability_chart.png             # Run-to-run variability
    └── layer_breakdown.png               # Silver vs Gold time breakdown
```

## Setup

### 1. Install Additional Dependencies (they are already in requirements.txt)

```bash
pip install matplotlib numpy
```

### 2. Configure Experiments (Optional)

Edit `experiments/optimization_config.yaml` to:
- Modify optimization settings
- Change number of runs per experiment
- Add new experiment configurations
- Change dataset size
- Adjust data generation parameters
- Set paths for data and results

## Running Experiments

### Full Experiment Suite

Run all experiments with default settings (3 runs each):

```bash
python experiments/run_experiments.py
```

This will:
1. Generate fresh test data (by default 10K users, 10K products, 18K transactions)
2. Run each experiment configuration (by default 3 times)
3. Collect performance metrics
4. Save results to JSON
5. Display summary analysis

### Custom Experiment Run

Edit `run_experiments.py` to customize:

```python
# Run only specific experiments
experiments_to_run = ['baseline', 'all_optimizations', 'caching_only']

# Change number of runs
runs_per_experiment = 5

# Disable warmup
warmup_run = False
```

### Quick Test

For a quick test with minimal runs:

```python
# In run_experiments.py, modify:
results = run_all_experiments(
    experiments_to_run=['baseline', 'all_optimizations'],
    runs_per_experiment=1,
    warmup_run=False
)
```

## Analyzing Results

### Automatic Analysis

The experiment runner automatically displays:
- Average execution times per experiment
- Silver layer vs Gold layer breakdown
- Performance improvements vs baseline
- Min/max execution times

### Generate Visualizations

After experiments complete, create charts:

```bash
# Uses most recent results file
python experiments/visualize_results.py
``` 

This creates 4 visualization charts:
1. **comparison_chart.png** - Stacked bar chart of all experiments
2. **speedup_chart.png** - Horizontal bar chart showing % improvement
3. **variability_chart.png** - Box plot showing consistency
4. **layer_breakdown.png** - Silver vs Gold layer time distribution

## Metrics

### Collected Metrics

Each experiment run collects:

- **Transformation Time**: Pure processing time (excludes I/O)
- **Total Execution Time**: Includes data loading and saving (for reference)
- **Silver Layer Transform Time**: Time for data cleaning and transformations
- **Gold Layer Transform Time**: Time for SQL query execution
- **Per-Table Transform Times**: Individual timing for each table in silver layer
- **Per-Query Transform Times**: Individual timing for each SQL query in gold layer

### Statistical Analysis

For each experiment (across multiple runs):
- **Average Transformation Time**: Mean transformation time (primary metric)
- **Average Total Time**: Includes I/O (for reference)
- **Standard Deviation**: Measure of consistency
- **Min/Max**: Best and worst case performance
- **Speedup**: Percentage improvement vs baseline

## Adding New Experiments

1. Edit `config/optimization_config.yaml`:

```yaml
experiments:
  custom_experiment:
    name: "My Custom Config"
    description: "Testing specific combination"
    optimizations:
      shuffle_partitions: 16
      enable_aqe: true
      enable_caching: false
      enable_coalesce: true
      compression: "snappy"
      broadcast_threshold: 5242880  # 5MB
      enable_partitioning: true
      partition_column: "category"  # for products table
```


