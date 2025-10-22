import pandas as pd
import numpy as np
from math import ceil

def sample_from_chunks(file_path, n_samples, chunk_size=100000):
    # Count total rows first
    total_rows = sum(1 for _ in open(file_path)) - 1  # subtract 1 for header
    
    # Calculate sampling probability
    prob = n_samples / total_rows
    
    # Initialize empty list for samples
    samples = []
    
    # Read and process chunks
    for chunk in pd.read_csv(file_path, chunksize=chunk_size):
        # Sample from this chunk with probability adjusted to target total samples
        chunk_sample = chunk.sample(n=ceil(len(chunk) * prob), random_state=42)
        samples.append(chunk_sample)
    
    # Concatenate all samples
    final_df = pd.concat(samples, ignore_index=True)
    
    # Take exactly n_samples rows from the combined sample
    return final_df.sample(n=min(n_samples, len(final_df)), random_state=42)

print("Starting to process Chicago taxi data...")
chicago_sample = sample_from_chunks('cityofchicago_taxi_data_2024.csv', n_samples=1000)
chicago_sample.to_csv('chicago_taxi_sample.csv', index=False)
print(f"Chicago sample shape: {chicago_sample.shape}")

print("\nStarting to process SF taxi data...")
sf_sample = sample_from_chunks('sfgov_taxi_data_2024.csv', n_samples=1000)
sf_sample.to_csv('sf_taxi_sample.csv', index=False)
print(f"SF sample shape: {sf_sample.shape}")

print("\nSamples have been extracted successfully!")