import json
import os
import sys
from pathlib import Path
from typing import Any, Union
import matplotlib.pyplot as plt
import numpy as np

# Add project root to path
project_root = str(Path(__file__).parents[1])
if project_root not in sys.path:
    sys.path.append(project_root)

from src.logging_utils.logger import logger
from experiments.config.optimization_experiment_config import optimization_config


def load_results(
    results_file: str = os.path.join(
        optimization_config.get_paths_config().get(
            "results_dir", "experiments/results"
        ),
        "experiment_results.json",
    ),
) -> list[dict[str, Any]]:
    """
    Load experiment results from JSON file
    Args:
        results_file (str): Path to the results file
    Returns:
        list[dict[str, Any]]: Loaded experiment results
    """
    with open(results_file, "r") as f:
        return json.load(f)


def prepare_data_for_visualization(results: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Prepare data for visualization
    Args:
        results (list[dict[str, Any]]): List of experiment results
    Returns:
        dict[str, Any]: Prepared data for visualization
    """
    # Group by experiment
    experiment_groups = {}
    for result in results:
        if result.get("status") != "success":
            continue

        exp_name = result["experiment_name"]
        if exp_name not in experiment_groups:
            experiment_groups[exp_name] = {
                "silver_times": [],
                "gold_times": [],
                "total_times": [],
                "description": result["config"].get("description", ""),
            }

        # Use transformation times (not including write I/O)
        experiment_groups[exp_name]["silver_times"].append(
            result["silver_layer"]["transformation_time"]
        )
        experiment_groups[exp_name]["gold_times"].append(
            result["gold_layer"]["transformation_time"]
        )
        experiment_groups[exp_name]["total_times"].append(
            result["total_transformation_time"]
        )

    # Calculate averages
    viz_data = {}
    for exp_name, data in experiment_groups.items():
        viz_data[exp_name] = {
            "avg_silver": np.mean(data["silver_times"]),
            "avg_gold": np.mean(data["gold_times"]),
            "avg_total": np.mean(data["total_times"]),
            "std_total": np.std(data["total_times"]),
            "description": data["description"],
            "all_runs": data["total_times"],
        }

    return viz_data


def create_comparison_chart(
    viz_data: dict[str, Any],
    output_file: str = os.path.join(
        optimization_config.get_paths_config().get(
            "results_dir", "experiments/results"
        ),
        "comparison_chart.png",
    ),
) -> None:
    """
    Create bar chart comparing all experiments
    Args:
        viz_data (dict[str, Any]): Prepared data for visualization
        output_file (str): Path to save the output chart
    """
    experiments = list(viz_data.keys())
    silver_times = [viz_data[exp]["avg_silver"] for exp in experiments]
    gold_times = [viz_data[exp]["avg_gold"] for exp in experiments]

    # Create figure
    fig, ax = plt.subplots(figsize=(14, 8))

    x = np.arange(len(experiments))
    width = 0.35

    # Create stacked bars
    bars1 = ax.bar(x, silver_times, width, label="Silver Layer", color="#4CAF50")
    bars2 = ax.bar(
        x, gold_times, width, bottom=silver_times, label="Gold Layer", color="#2196F3"
    )

    # Customize
    ax.set_xlabel("Experiment Configuration", fontsize=12, fontweight="bold")
    ax.set_ylabel("Transformation Time (seconds)", fontsize=12, fontweight="bold")
    ax.set_title(
        "Spark Optimization Experiments - Transformation Time Comparison\n(Includes DAG building + execution via count(), excludes write I/O)",
        fontsize=14,
        fontweight="bold",
        pad=20,
    )
    ax.set_xticks(x)
    ax.set_xticklabels(experiments, rotation=45, ha="right")
    ax.legend(loc="upper left", fontsize=10)
    ax.grid(axis="y", alpha=0.3, linestyle="--")

    # Add value labels on bars
    total_times = [silver_times[i] + gold_times[i] for i in range(len(experiments))]
    for i, total in enumerate(total_times):
        ax.text(
            i,
            total + 0.5,
            f"{total:.1f}s",
            ha="center",
            va="bottom",
            fontsize=9,
            fontweight="bold",
        )

    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    logger.info(f"Comparison chart saved to: {output_file}")
    plt.close()


def create_speedup_chart(
    viz_data: dict[str, Any],
    output_file: str = os.path.join(
        optimization_config.get_paths_config().get(
            "results_dir", "experiments/results"
        ),
        "speedup_chart.png",
    ),
) -> None:
    """
    Create chart showing speedup compared to baseline (X times faster)
    Args:
        viz_data (dict[str, Any]): Prepared data for visualization
        output_file (str): Path to save the output chart
    """
    # Find baseline
    baseline_time = None
    for exp_name, data in viz_data.items():
        if "baseline" in exp_name.lower():
            baseline_time = data["avg_total"]
            break

    if baseline_time is None:
        logger.warning("No baseline experiment found, skipping speedup chart")
        return

    # Calculate speedups (X times faster)
    experiments = []
    speedups = []
    colors = []

    for exp_name, data in viz_data.items():
        if "baseline" in exp_name.lower():
            continue

        # Calculate how many times faster (baseline_time / experiment_time)
        times_faster = baseline_time / data["avg_total"]
        experiments.append(exp_name)
        speedups.append(times_faster)
        colors.append("#4CAF50" if times_faster > 1.0 else "#F44336")

    # Create figure
    fig, ax = plt.subplots(figsize=(12, 8))

    y_pos = np.arange(len(experiments))
    bars = ax.barh(
        y_pos, speedups, color=colors, alpha=0.8, edgecolor="black", linewidth=1.5
    )

    # Add value labels
    for i, (bar, speedup) in enumerate(zip(bars, speedups)):
        width = bar.get_width()
        label_x = width + 0.2 if width > 1.0 else width - 0.2
        ax.text(
            label_x,
            bar.get_y() + bar.get_height() / 2,
            f"{speedup:.1f}x",
            ha="left" if width > 1.0 else "right",
            va="center",
            fontsize=10,
            fontweight="bold",
        )

    # Customize
    ax.set_yticks(y_pos)
    ax.set_yticklabels(experiments)
    ax.set_xlabel(
        "Speedup (times faster than baseline)", fontsize=12, fontweight="bold"
    )
    ax.set_title(
        f"Performance vs Baseline\n(Baseline: {baseline_time:.2f}s transformation time)",
        fontsize=14,
        fontweight="bold",
        pad=20,
    )
    ax.axvline(
        x=1.0, color="black", linewidth=2, linestyle="-", label="Baseline (1.0x)"
    )
    ax.grid(axis="x", alpha=0.3, linestyle="--")

    # Add legend
    from matplotlib.patches import Patch

    legend_elements = [
        Patch(facecolor="#4CAF50", label="Faster than baseline"),
        Patch(facecolor="#F44336", label="Slower than baseline"),
    ]
    ax.legend(handles=legend_elements, loc="lower right", fontsize=10)

    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    logger.info(f"Speedup chart saved to: {output_file}")
    plt.close()


def create_variability_chart(
    viz_data: dict[str, Any],
    output_dir: str = optimization_config.get_paths_config().get(
        "results_dir", "experiments/results"
    ),
) -> None:
    """
    Create individual box plots for each experiment showing run-to-run variability
    Args:
        viz_data (dict[str, Any]): Prepared data for visualization
        output_dir (str): Directory to save the output charts
    """
    # Create subdirectory for variability plots
    variability_dir = os.path.join(output_dir, "variability_plots")
    os.makedirs(variability_dir, exist_ok=True)

    for exp_name, data in viz_data.items():
        all_runs = data["all_runs"]

        # Create figure for this experiment
        fig, ax = plt.subplots(figsize=(8, 6))

        bp = ax.boxplot(
            [all_runs],
            patch_artist=True,
            showmeans=True,
            meanline=True,
            boxprops=dict(facecolor="#2196F3", alpha=0.7),
            medianprops=dict(color="red", linewidth=2),
            meanprops=dict(color="green", linewidth=2, linestyle="--"),
            whiskerprops=dict(linewidth=1.5),
            capprops=dict(linewidth=1.5),
        )

        # Customize
        ax.set_ylabel(
            "Total Transformation Time (seconds)", fontsize=12, fontweight="bold"
        )
        ax.set_title(
            f"Transformation Time Variability: {exp_name}\n(Includes DAG building + execution, excludes write I/O)",
            fontsize=13,
            fontweight="bold",
            pad=20,
        )
        ax.set_xticklabels([exp_name])
        ax.grid(axis="y", alpha=0.3, linestyle="--")

        # Add statistics text
        mean_time = np.mean(all_runs)
        median_time = np.median(all_runs)
        std_time = np.std(all_runs)
        min_time = np.min(all_runs)
        max_time = np.max(all_runs)

        stats_text = (
            f"Mean: {mean_time:.2f}s\n"
            f"Median: {median_time:.2f}s\n"
            f"Std Dev: {std_time:.2f}s\n"
            f"Min: {min_time:.2f}s\n"
            f"Max: {max_time:.2f}s"
        )

        ax.text(
            0.98,
            0.98,
            stats_text,
            transform=ax.transAxes,
            fontsize=10,
            verticalalignment="top",
            horizontalalignment="right",
            bbox=dict(boxstyle="round", facecolor="wheat", alpha=0.5),
        )

        # Add legend
        from matplotlib.lines import Line2D

        legend_elements = [
            Line2D([0], [0], color="red", linewidth=2, label="Median"),
            Line2D([0], [0], color="green", linewidth=2, linestyle="--", label="Mean"),
        ]
        ax.legend(handles=legend_elements, loc="upper left", fontsize=10)

        plt.tight_layout()

        # Save individual plot
        output_file = os.path.join(variability_dir, f"{exp_name}_variability.png")
        plt.savefig(output_file, dpi=300, bbox_inches="tight")
        logger.info(f"Variability chart for {exp_name} saved to: {output_file}")
        plt.close()

    logger.info(f"All variability charts saved to: {variability_dir}")


def create_layer_breakdown_chart(
    viz_data: dict[str, Any],
    output_file: str = os.path.join(
        optimization_config.get_paths_config().get(
            "results_dir", "experiments/results"
        ),
        "layer_breakdown.png",
    ),
) -> None:
    """
    Create chart showing silver vs gold layer time breakdown
    Args:
        viz_data (dict[str, Any]): Prepared data for visualization
        output_file (str): Path to save the output chart
    """
    experiments = list(viz_data.keys())
    silver_times = [viz_data[exp]["avg_silver"] for exp in experiments]
    gold_times = [viz_data[exp]["avg_gold"] for exp in experiments]

    # Calculate percentages
    total_times = [s + g for s, g in zip(silver_times, gold_times)]
    silver_pct = [(s / t) * 100 for s, t in zip(silver_times, total_times)]
    gold_pct = [(g / t) * 100 for g, t in zip(gold_times, total_times)]

    # Create figure with two subplots
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(16, 8))

    # Subplot 1: Absolute times
    x = np.arange(len(experiments))
    width = 0.6

    ax1.bar(x, silver_times, width, label="Silver Layer", color="#4CAF50", alpha=0.8)
    ax1.bar(
        x,
        gold_times,
        width,
        bottom=silver_times,
        label="Gold Layer",
        color="#2196F3",
        alpha=0.8,
    )

    ax1.set_xlabel("Experiment Configuration", fontsize=12, fontweight="bold")
    ax1.set_ylabel("Transformation Time (seconds)", fontsize=12, fontweight="bold")
    ax1.set_title("Layer Transformation Time Breakdown", fontsize=13, fontweight="bold")
    ax1.set_xticks(x)
    ax1.set_xticklabels(experiments, rotation=45, ha="right")
    ax1.legend(loc="upper left", fontsize=10)
    ax1.grid(axis="y", alpha=0.3, linestyle="--")

    # Subplot 2: Percentage breakdown
    ax2.bar(x, silver_pct, width, label="Silver Layer", color="#4CAF50", alpha=0.8)
    ax2.bar(
        x,
        gold_pct,
        width,
        bottom=silver_pct,
        label="Gold Layer",
        color="#2196F3",
        alpha=0.8,
    )

    ax2.set_xlabel("Experiment Configuration", fontsize=12, fontweight="bold")
    ax2.set_ylabel("Percentage of Total Time (%)", fontsize=12, fontweight="bold")
    ax2.set_title(
        "Layer Transformation Time Distribution", fontsize=13, fontweight="bold"
    )
    ax2.set_xticks(x)
    ax2.set_xticklabels(experiments, rotation=45, ha="right")
    ax2.legend(loc="upper left", fontsize=10)
    ax2.grid(axis="y", alpha=0.3, linestyle="--")
    ax2.set_ylim([0, 100])

    plt.tight_layout()
    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    logger.info(f"Layer breakdown chart saved to: {output_file}")
    plt.close()


def main(results_file: Union[str, None] = None) -> None:
    """
    Main visualization function
    Args:
        results_file (str): Path to the experiment results file
    """
    # Find most recent results file if not specified
    if results_file is None:
        results_dir = Path("experiments/results")
        json_files = list(results_dir.glob("experiment_results_*.json"))

        if not json_files:
            logger.error("No experiment results found!")
            return

        results_file = str(max(json_files, key=lambda p: p.stat().st_mtime))
        logger.info(f"Using most recent results: {results_file}")

    # Load and prepare data
    logger.info("Loading experiment results...")
    results = load_results(results_file)
    viz_data = prepare_data_for_visualization(results)

    if not viz_data:
        logger.error("No successful experiment runs found!")
        return

    # Create visualizations
    logger.info("Creating visualizations...")
    create_comparison_chart(viz_data)
    create_speedup_chart(viz_data)
    create_variability_chart(viz_data)
    create_layer_breakdown_chart(viz_data)

    logger.info("\nAll visualizations created successfully!")


if __name__ == "__main__":
    # Provide path to your results file if you want to specify it
    results_file = None
    main(results_file)
