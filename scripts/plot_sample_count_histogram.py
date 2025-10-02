#!/usr/bin/env python3
"""
Efficient histogram plotting for sample_count distribution from large parquet file.
Processes data in chunks to handle 32+ billion rows.
"""

import pyarrow.parquet as pq
import numpy as np
import matplotlib.pyplot as plt
from tqdm import tqdm
import argparse
import os

def create_histogram_efficient(parquet_path, num_bins=50, max_value=None):
    """
    Create histogram of sample_count distribution efficiently for large datasets.
    
    Parameters:
    -----------
    parquet_path : str
        Path to the parquet file
    num_bins : int
        Number of bins for the histogram
    max_value : int, optional
        Maximum value to include (useful for limiting outliers)
    """
    
    parquet_file = pq.ParquetFile(parquet_path)
    num_rows = parquet_file.metadata.num_rows
    num_row_groups = parquet_file.metadata.num_row_groups
    
    print(f"Total rows: {num_rows:,}")
    print(f"Number of row groups: {num_row_groups}")
    
    # First pass: determine the range of values
    print("\nFirst pass: Finding min/max values...")
    min_val = float('inf')
    max_val = float('-inf')
    
    for i in tqdm(range(num_row_groups), desc="Scanning row groups"):
        table = parquet_file.read_row_group(i, columns=['sample_count'])
        sample_counts = table.column('sample_count').to_numpy()
        
        min_val = min(min_val, sample_counts.min())
        max_val = max(max_val, sample_counts.max())
    
    print(f"Min sample_count: {min_val}")
    print(f"Max sample_count: {max_val}")
    
    # Apply max_value limit if specified
    if max_value is not None:
        max_val = min(max_val, max_value)
        print(f"Using max_value: {max_val}")
    
    # Define bin edges
    bin_edges = np.linspace(min_val, max_val, num_bins + 1)
    hist_counts = np.zeros(num_bins, dtype=np.int64)
    
    # Second pass: accumulate histogram counts
    print("\nSecond pass: Building histogram...")
    for i in tqdm(range(num_row_groups), desc="Processing row groups"):
        table = parquet_file.read_row_group(i, columns=['sample_count'])
        sample_counts = table.column('sample_count').to_numpy()
        
        # Filter out values above max_value if specified
        if max_value is not None:
            sample_counts = sample_counts[sample_counts <= max_value]
        
        # Compute histogram for this chunk and add to total
        chunk_hist, _ = np.histogram(sample_counts, bins=bin_edges)
        hist_counts += chunk_hist
    
    return hist_counts, bin_edges, min_val, max_val


def plot_histogram(hist_counts, bin_edges, save_path='sample_count_histogram.png'):
    """
    Create and save histogram plot.
    
    Parameters:
    -----------
    hist_counts : np.array
        Histogram counts for each bin
    bin_edges : np.array
        Edges of histogram bins
    save_path : str
        Path to save the plot
    """
    
    fig, ax = plt.subplots(figsize=(12, 6))
    
    # Calculate bin centers for plotting
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
    bin_widths = bin_edges[1:] - bin_edges[:-1]
    
    # Create bar plot
    ax.bar(bin_centers, hist_counts, width=bin_widths, 
           edgecolor='black', alpha=0.7, color='steelblue')
    
    ax.set_xlabel('Sample Count', fontsize=12)
    ax.set_ylabel('Frequency', fontsize=12)
    ax.set_title('Distribution of Sample Count', fontsize=14, fontweight='bold')
    ax.grid(axis='y', alpha=0.3)
    
    # Format y-axis with comma separators
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
    
    plt.tight_layout()
    plt.savefig(save_path, dpi=300, bbox_inches='tight')
    print(f"\nHistogram saved to: {save_path}")
    
    # Also create a log-scale version
    fig, ax = plt.subplots(figsize=(12, 6))
    ax.bar(bin_centers, hist_counts, width=bin_widths, 
           edgecolor='black', alpha=0.7, color='steelblue')
    ax.set_xlabel('Sample Count', fontsize=12)
    ax.set_ylabel('Frequency (log scale)', fontsize=12)
    ax.set_title('Distribution of Sample Count (Log Scale)', fontsize=14, fontweight='bold')
    ax.set_yscale('log')
    ax.grid(axis='y', alpha=0.3, which='both')
    
    plt.tight_layout()
    log_path = save_path.replace('.png', '_log.png')
    plt.savefig(log_path, dpi=300, bbox_inches='tight')
    print(f"Log-scale histogram saved to: {log_path}")
    
    return fig


def print_statistics(hist_counts, bin_edges):
    """Print summary statistics."""
    total_count = hist_counts.sum()
    bin_centers = (bin_edges[:-1] + bin_edges[1:]) / 2
    
    # Calculate weighted mean and median
    weighted_mean = np.sum(bin_centers * hist_counts) / total_count
    
    # Find median bin
    cumsum = np.cumsum(hist_counts)
    median_idx = np.searchsorted(cumsum, total_count / 2)
    median_approx = bin_centers[median_idx] if median_idx < len(bin_centers) else bin_centers[-1]
    
    print(f"\n{'='*50}")
    print("SUMMARY STATISTICS")
    print(f"{'='*50}")
    print(f"Total records: {total_count:,}")
    print(f"Approximate mean sample_count: {weighted_mean:.2f}")
    print(f"Approximate median sample_count: {median_approx:.2f}")
    print(f"Min sample_count: {bin_edges[0]:.0f}")
    print(f"Max sample_count: {bin_edges[-1]:.0f}")
    print(f"\nMost common bin: {bin_centers[np.argmax(hist_counts)]:.0f} (count: {hist_counts.max():,})")
    print(f"{'='*50}\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='Create histogram of sample_count distribution from large parquet file',
        formatter_class=argparse.ArgumentDefaultsHelpFormatter
    )
    
    parser.add_argument(
        '-i', '--input',
        required=True,
        help='Path to input parquet file'
    )
    
    parser.add_argument(
        '-o', '--output',
        required=True,
        help='Path to output histogram image (e.g., histogram.png)'
    )
    
    parser.add_argument(
        '-b', '--bins',
        type=int,
        default=100,
        help='Number of histogram bins'
    )
    
    parser.add_argument(
        '-m', '--max-value',
        type=int,
        default=None,
        help='Maximum sample_count value to include (useful for excluding outliers)'
    )
    
    args = parser.parse_args()
    
    # Validate input file exists
    if not os.path.exists(args.input):
        print(f"Error: Input file not found: {args.input}")
        exit(1)
    
    # Create output directory if it doesn't exist
    output_dir = os.path.dirname(args.output)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")
    
    print(f"Input file: {args.input}")
    print(f"Output file: {args.output}")
    print(f"Number of bins: {args.bins}")
    if args.max_value:
        print(f"Max value limit: {args.max_value}")
    print()
    
    # Process and create histogram
    hist_counts, bin_edges, min_val, max_val = create_histogram_efficient(
        args.input, 
        num_bins=args.bins,
        max_value=args.max_value
    )
    
    # Print statistics
    print_statistics(hist_counts, bin_edges)
    
    # Create and save plots
    plot_histogram(hist_counts, bin_edges, save_path=args.output)
    
    print("\nDone!")

