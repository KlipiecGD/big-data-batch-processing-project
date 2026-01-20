import sys
import os
import json
import time
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Any, Optional

# Add project root to path
project_root = str(Path(__file__).parents[1])
if project_root not in sys.path:
    sys.path.append(project_root)

from experiments.config.optimization_experiment_config import optimization_config
from experiments.process_silver_layer_experiment import run_silver_layer_experiment
from experiments.process_gold_layer_experiment import run_gold_layer_experiment
from src.data_generation.generate_bronze_layer_data import generate_transactions_dataset
from src.logging_utils.logger import logger


def setup_experiment_directories() -> None:
    """Create necessary directories for experiments"""
    dirs = [
        optimization_config.get_paths_config().get(
            "bronze_layer_dir", "experiments/data/bronze_layer/"
        ),
        optimization_config.get_paths_config().get(
            "silver_layer_dir", "experiments/data/silver_layer/"
        ),
        optimization_config.get_paths_config().get(
            "gold_layer_dir", "experiments/data/gold_layer/"
        ),
        optimization_config.get_paths_config().get(
            "results_dir", "experiments/results/"
        ),
    ]

    for dir_path in dirs:
        Path(dir_path).mkdir(parents=True, exist_ok=True)
        logger.info(f"Created directory: {dir_path}")


def generate_test_data() -> None:
    """Generate bronze layer data for experiments"""
    logger.info("Generating test data for experiments...")

    # Generate transactions dataset
    generate_transactions_dataset(
        users_count=optimization_config.get_execution_config().get(
            "users_count", 10000
        ),
        products_count=optimization_config.get_execution_config().get(
            "products_count", 10000
        ),
        transactions_count=optimization_config.get_execution_config().get(
            "transactions_count", 18000
        ),
        noise_level=optimization_config.get_execution_config().get("noise_level", 0.03),
        null_wrong_proportion=optimization_config.get_execution_config().get(
            "null_wrong_proportion", 0.5
        ),
        save_locally=True,
        save_to_cloud=False,
        data_path=optimization_config.get_paths_config().get(
            "bronze_layer_dir", "experiments/data/bronze_layer/"
        ),
    )

    logger.info("Test data generation completed")


def run_single_experiment(
    experiment_name: str, experiment_config: dict[str, Any], run_number: int
) -> dict[str, Any]:
    """
    Run a single experiment (silver + gold layer)

    Args:
        experiment_name (str): Name of the experiment
        experiment_config (dict[str, Any]): Configuration for the experiment
        run_number (int): Run number (for multiple runs)

    Returns:
        dictionary with all metrics from both layers
    """
    logger.info(f"\n{'=' * 60}")
    logger.info(f"Running Experiment: {experiment_name} (Run {run_number})")
    logger.info(f"Description: {experiment_config.get('description', 'N/A')}")
    logger.info(f"{'=' * 60}\n")

    experiment_metrics = {
        "experiment_name": experiment_name,
        "run_number": run_number,
        "timestamp": datetime.now().isoformat(),
        "config": experiment_config,
    }

    try:
        # Run silver layer
        silver_metrics = run_silver_layer_experiment(
            optimization_config=experiment_config,
            experiment_name=f"{experiment_name}_run{run_number}",
            bronze_path=optimization_config.get_paths_config().get(
                "bronze_layer_dir", "experiments/data/bronze_layer/"
            ),
            silver_path=optimization_config.get_paths_config().get(
                "silver_layer_dir", "experiments/data/silver_layer/"
            ),
        )
        experiment_metrics["silver_layer"] = silver_metrics

        # Wait a bit between stages
        time.sleep(2)

        # Run gold layer
        gold_metrics = run_gold_layer_experiment(
            optimization_config=experiment_config,
            experiment_name=f"{experiment_name}_run{run_number}",
            silver_path=optimization_config.get_paths_config().get(
                "silver_layer_dir", "experiments/data/silver_layer/"
            ),
            gold_path=optimization_config.get_paths_config().get(
                "gold_layer_dir", "experiments/data/gold_layer/"
            ),
        )
        experiment_metrics["gold_layer"] = gold_metrics

        # Calculate total time (transformation only)
        total_transform_time = silver_metrics.get(
            "transformation_time", 0
        ) + gold_metrics.get("transformation_time", 0)
        # Calculate total time including write I/O
        total_time_with_io = silver_metrics.get("write_time", 0) + gold_metrics.get(
            "write_time", 0
        )
        # Calculate total time including spark initialization and write I/O
        total_time = silver_metrics.get("total_execution_time", 0) + gold_metrics.get(
            "total_execution_time", 0
        )

        experiment_metrics["total_transformation_time"] = total_transform_time
        experiment_metrics["total_time_with_io"] = total_time_with_io
        experiment_metrics["total_time"] = total_time
        experiment_metrics["status"] = "success"

        logger.info(f"\n{'=' * 60}")
        logger.info(
            f"Experiment {experiment_name} (Run {run_number}) Completed Successfully!"
        )
        logger.info(
            f"Silver Layer Transform Time: {silver_metrics.get('transformation_time', 0):.2f}s"
        )
        logger.info(
            f"Gold Layer Transform Time: {gold_metrics.get('transformation_time', 0):.2f}s"
        )
        logger.info(f"Total Transform Time: {total_transform_time:.2f}s")
        logger.info(f"Total Time (with I/O): {total_time_with_io:.2f}s")
        logger.info(
            f"Total Time (with Spark Initialization and I/O): {total_time:.2f}s"
        )
        logger.info(f"{'=' * 60}\n")

    except Exception as e:
        logger.error(f"Experiment {experiment_name} (Run {run_number}) failed: {e}")
        experiment_metrics["status"] = "failed"
        experiment_metrics["error"] = str(e)

    return experiment_metrics


def run_all_experiments(
    experiments_to_run: list[str] = ["all"],
    runs_per_experiment: int = optimization_config.get_execution_config().get(
        "runs_per_experiment", 3
    ),
    warmup_run: bool = optimization_config.get_execution_config().get(
        "warmup_runs", True
    ),
) -> list[dict[str, Any]]:
    """
    Run all experiments or a subset of experiments

    Args:
        experiments_to_run (list[str]): list of experiment names to run, if ['all'] then run all
        runs_per_experiment (int): Number of times to run each experiment
        warmup_run (bool): Whether to do a warmup run first

    Returns:
        list[dict[str, Any]]: List of all experiment results
    """
    # Load configuration
    all_experiments = optimization_config.get_all_experiments()

    # Filter experiments if specified
    if experiments_to_run[0] != "all":
        experiments = {
            k: v for k, v in all_experiments.items() if k in experiments_to_run
        }
    else:
        experiments = all_experiments

    logger.info(f"\n{'=' * 60}")
    logger.info("STARTING OPTIMIZATION EXPERIMENTS")
    logger.info(f"Total Experiments: {len(experiments)}")
    logger.info(f"Runs per Experiment: {runs_per_experiment}")
    logger.info(f"Warmup Run: {warmup_run}")
    logger.info(f"{'=' * 60}\n")

    all_results = []

    # Warmup run with baseline
    if warmup_run and "baseline" in experiments:
        logger.info("Running warmup with baseline configuration...")
        _ = run_single_experiment(
            "baseline_warmup", experiments["baseline"], 0
        )
        logger.info("Warmup completed\n")

    # Run all experiments
    for exp_name, exp_config in experiments.items():
        for run_num in range(1, runs_per_experiment + 1):
            result = run_single_experiment(exp_name, exp_config, run_num)
            all_results.append(result)

            # Short pause between experiments
            time.sleep(2)

    return all_results


def save_results(
    results: list[dict[str, Any]], output_file: Optional[str] = None
) -> None:
    """
    Save experiment results to JSON file
    Args:
        results (list[dict[str, Any]]): List of experiment results
        output_file (Optional[str]): Path to output file, if None generate with timestamp
    """
    if output_file is None:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_file = os.path.join(
            optimization_config.get_paths_config().get(
                "results_dir", "experiments/results/"
            ),
            f"experiment_results_{timestamp}.json",
        )

    with open(output_file, "w") as f:
        json.dump(results, f, indent=2)

    logger.info(f"\nResults saved to: {output_file}")


def analyze_results(results: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Generate summary analysis of results
    Args:
        results (list[dict[str, Any]]): List of experiment results
    Returns:
        list[dict[str, Any]]: Summary statistics for each experiment
    """
    logger.info("\n" + "=" * 60)
    logger.info("EXPERIMENT RESULTS SUMMARY")
    logger.info("=" * 60 + "\n")

    # Group results by experiment
    experiment_groups = {}
    for result in results:
        exp_name = result["experiment_name"]
        if exp_name not in experiment_groups:
            experiment_groups[exp_name] = []
        experiment_groups[exp_name].append(result)

    # Calculate statistics for each experiment
    summary = []
    for exp_name, exp_results in experiment_groups.items():
        successful_runs = [r for r in exp_results if r.get("status") == "success"]

        if not successful_runs:
            logger.warning(f"{exp_name}: All runs failed")
            continue

        # Calculate average times (transformation only, not write I/O)
        silver_times = [
            r["silver_layer"]["transformation_time"] for r in successful_runs
        ]
        gold_times = [r["gold_layer"]["transformation_time"] for r in successful_runs]
        total_times = [r["total_transformation_time"] for r in successful_runs]

        # Also track write times for reference
        silver_write_times = [
            r["silver_layer"].get("write_time", 0) for r in successful_runs
        ]
        gold_write_times = [
            r["gold_layer"].get("write_time", 0) for r in successful_runs
        ]

        avg_silver = sum(silver_times) / len(silver_times)
        avg_gold = sum(gold_times) / len(gold_times)
        avg_total = sum(total_times) / len(total_times)

        avg_silver_write = sum(silver_write_times) / len(silver_write_times)
        avg_gold_write = sum(gold_write_times) / len(gold_write_times)
        avg_total_write = avg_silver_write + avg_gold_write

        min_total = min(total_times)
        max_total = max(total_times)
        std_total = np.std(total_times) if len(total_times) > 1 else 0.0

        summary.append(
            {
                "experiment": exp_name,
                "description": exp_results[0]["config"].get("description", "N/A"),
                "successful_runs": len(successful_runs),
                "avg_silver_time": avg_silver,
                "avg_gold_time": avg_gold,
                "avg_total_time": avg_total,
                "avg_total_write_time": avg_total_write,
                "min_total_time": min_total,
                "max_total_time": max_total,
                "std_total_time": std_total,
            }
        )

    # Sort by average total time
    summary.sort(key=lambda x: x["avg_total_time"])

    # Print summary table
    logger.info(
        f"{'Experiment':<30} {'Avg Silver':<12} {'Avg Gold':<12} {'Avg Total':<12} {'Min':<10} {'Max':<10} {'Std Dev':<10}"
    )
    logger.info("-" * 105)

    for s in summary:
        logger.info(
            f"{s['experiment']:<30} "
            f"{s['avg_silver_time']:>10.2f}s  "
            f"{s['avg_gold_time']:>10.2f}s  "
            f"{s['avg_total_time']:>10.2f}s  "
            f"{s['min_total_time']:>8.2f}s  "
            f"{s['max_total_time']:>8.2f}s  "
            f"{s['std_total_time']:>8.2f}s"
        )
    logger.info(
        "\nNote: Times shown are transformation only (DAG + execution via count(), excludes write I/O)"
    )

    # Calculate improvements
    if summary:
        baseline_time = next(
            (s["avg_total_time"] for s in summary if "baseline" in s["experiment"]),
            None,
        )

        if baseline_time:
            logger.info("\n" + "=" * 60)
            logger.info("PERFORMANCE IMPROVEMENTS vs BASELINE")
            logger.info("=" * 60 + "\n")
            logger.info(f"{'Experiment':<30} {'Times Faster':<15} {'Time Savings':<15}")
            logger.info("-" * 60)

            for s in summary:
                if "baseline" not in s["experiment"]:
                    times_faster = baseline_time / s["avg_total_time"]
                    time_saved = baseline_time - s["avg_total_time"]
                    logger.info(
                        f"{s['experiment']:<30} "
                        f"{times_faster:>13.2f}x  "
                        f"{time_saved:>13.2f}s"
                    )

            logger.info("\n" + "=" * 60)
            logger.info("WRITE TIME SUMMARY (for reference)")
            logger.info("=" * 60 + "\n")
            logger.info(f"{'Experiment':<30} {'Avg Write Time':<15}")
            logger.info("-" * 45)

            for s in summary:
                logger.info(
                    f"{s['experiment']:<30} {s['avg_total_write_time']:>13.2f}s"
                )

    return summary


def main():
    """Main execution function"""
    # Setup
    logger.info("Setting up experiment environment...")
    setup_experiment_directories()

    # Generate test data
    generate_test_data()

    # Run experiments
    # You can specify which experiments to run:
    # experiments_to_run = ['baseline', 'all_optimizations']
    # Or run all experiments:
    experiments_to_run = ["all"]

    results = run_all_experiments(
        experiments_to_run=experiments_to_run,
        runs_per_experiment=optimization_config.get_execution_config().get(
            "runs_per_experiment", 3
        ),
        warmup_run=optimization_config.get_execution_config().get("warmup_runs", True),
    )

    # Save results
    save_results(results)

    # Analyze and display results
    analyze_results(results)

    logger.info("\n" + "=" * 60)
    logger.info("ALL EXPERIMENTS COMPLETED!")
    logger.info("=" * 60 + "\n")


if __name__ == "__main__":
    main()
