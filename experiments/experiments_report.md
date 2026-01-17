2026-01-16 21:26:29,031 - app - INFO - run_experiments.py - analyze_results - EXPERIMENT RESULTS SUMMARY
2026-01-16 21:26:29,031 - app - INFO - run_experiments.py - analyze_results - ================================================================================

2026-01-16 21:26:29,031 - app - INFO - run_experiments.py - analyze_results - Experiment                     Avg Silver   Avg Gold     Avg Total    Min        Max       
2026-01-16 21:26:29,031 - app - INFO - run_experiments.py - analyze_results - -----------------------------------------------------------------------------------------------
2026-01-16 21:26:29,031 - app - INFO - run_experiments.py - analyze_results - all_optimizations                    1.10s        1.56s        2.66s      2.59s      2.72s
2026-01-16 21:26:29,031 - app - INFO - run_experiments.py - analyze_results - shuffle_only                         1.14s        2.47s        3.61s      3.46s      3.83s
2026-01-16 21:26:29,031 - app - INFO - run_experiments.py - analyze_results - aqe_only                             1.23s        3.20s        4.43s      4.21s      4.76s
2026-01-16 21:26:29,031 - app - INFO - run_experiments.py - analyze_results - broadcast_only                       1.05s        9.38s       10.42s     10.09s     10.97s
2026-01-16 21:26:29,031 - app - INFO - run_experiments.py - analyze_results - caching_only                         5.24s       39.16s       44.39s     43.78s     45.09s
2026-01-16 21:26:29,032 - app - INFO - run_experiments.py - analyze_results - compression_only                     5.42s       39.28s       44.70s     42.96s     47.13s
2026-01-16 21:26:29,032 - app - INFO - run_experiments.py - analyze_results - partitioning_only                    5.14s       39.61s       44.75s     44.20s     45.54s
2026-01-16 21:26:29,032 - app - INFO - run_experiments.py - analyze_results - baseline                             5.53s       40.02s       45.56s     43.90s     48.44s
2026-01-16 21:26:29,032 - app - INFO - run_experiments.py - analyze_results - 
Note: Times shown are transformation only (excludes data loading/saving I/O)
2026-01-16 21:26:29,032 - app - INFO - run_experiments.py - analyze_results - 
================================================================================
2026-01-16 21:26:29,032 - app - INFO - run_experiments.py - analyze_results - PERFORMANCE IMPROVEMENTS vs BASELINE
2026-01-16 21:26:29,032 - app - INFO - run_experiments.py - analyze_results - ================================================================================

2026-01-16 21:26:29,032 - app - INFO - run_experiments.py - analyze_results - all_optimizations               94.15% faster
2026-01-16 21:26:29,032 - app - INFO - run_experiments.py - analyze_results - shuffle_only                    92.08% faster
2026-01-16 21:26:29,032 - app - INFO - run_experiments.py - analyze_results - aqe_only                        90.28% faster
2026-01-16 21:26:29,032 - app - INFO - run_experiments.py - analyze_results - broadcast_only                  77.12% faster
2026-01-16 21:26:29,032 - app - INFO - run_experiments.py - analyze_results - caching_only                     2.55% faster
2026-01-16 21:26:29,032 - app - INFO - run_experiments.py - analyze_results - compression_only                 1.88% faster
2026-01-16 21:26:29,032 - app - INFO - run_experiments.py - analyze_results - partitioning_only                1.78% faster
2026-01-16 21:26:29,032 - app - INFO - run_experim