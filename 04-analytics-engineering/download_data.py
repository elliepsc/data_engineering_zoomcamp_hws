#!/usr/bin/env python3
"""
Download NYC Taxi Data for Module 4 Homework
Downloads Green, Yellow (2019-2020) and FHV (2019) data
"""

import requests
from pathlib import Path
from tqdm import tqdm

def download_file(url: str, output_path: Path) -> bool:
    """Download file with progress bar"""
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        total_size = int(response.headers.get('content-length', 0))
        
        with open(output_path, 'wb') as f, tqdm(
            desc=output_path.name,
            total=total_size,
            unit='iB',
            unit_scale=True,
            unit_divisor=1024,
        ) as pbar:
            for chunk in response.iter_content(chunk_size=8192):
                size = f.write(chunk)
                pbar.update(size)
        
        return True
    except Exception as e:
        print(f"❌ Error downloading {output_path.name}: {e}")
        return False

def main():
    # Create data directory
    data_dir = Path("data/raw")
    data_dir.mkdir(parents=True, exist_ok=True)
    
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    
    files_to_download = []
    
    # Green taxi 2019-2020
    print("📊 Preparing Green taxi downloads (2019-2020)...")
    for year in [2019, 2020]:
        for month in range(1, 13):
            filename = f"green_tripdata_{year}-{month:02d}.parquet"
            files_to_download.append(("green", filename))
    
    # Yellow taxi 2019-2020
    print("📊 Preparing Yellow taxi downloads (2019-2020)...")
    for year in [2019, 2020]:
        for month in range(1, 13):
            filename = f"yellow_tripdata_{year}-{month:02d}.parquet"
            files_to_download.append(("yellow", filename))
    
    # FHV 2019
    print("📊 Preparing FHV downloads (2019)...")
    for month in range(1, 13):
        filename = f"fhv_tripdata_2019-{month:02d}.parquet"
        files_to_download.append(("fhv", filename))
    
    # Download files
    print(f"\n🚀 Starting download of {len(files_to_download)} files...\n")
    
    success_count = 0
    skip_count = 0
    
    for dataset_type, filename in files_to_download:
        output_path = data_dir / filename
        
        # Skip if already exists
        if output_path.exists():
            print(f"⏭️  Skipping {filename} (already exists)")
            skip_count += 1
            continue
        
        url = f"{base_url}/{filename}"
        
        if download_file(url, output_path):
            success_count += 1
    
    # Summary
    print(f"\n✅ Download complete!")
    print(f"   - Downloaded: {success_count} files")
    print(f"   - Skipped: {skip_count} files (already existed)")
    print(f"   - Total: {success_count + skip_count} files")
    print(f"\n📁 Data saved to: {data_dir.absolute()}")

if __name__ == "__main__":
    main()
