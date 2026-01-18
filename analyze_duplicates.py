
import csv
import os
import sys
from pathlib import Path

DATA_DIR = "/Users/a1234/Downloads/project-Whick/data"

def analyze_duplicates():
    print(f"📊 Analyzing data in {DATA_DIR}...")
    
    files = [f for f in os.listdir(DATA_DIR) if f.endswith('.csv')]
    files.sort()
    
    if not files:
        print("❌ No CSV files found.")
        return

    seen_ids = set()
    total_stats = {
        "files_processed": 0,
        "total_rows": 0,
        "unique_rows": 0,
        "duplicate_rows": 0
    }

    print("-" * 80)
    print(f"{'File Name':<30} | {'Rows':<12} | {'New Unique':<12} | {'Duplicates':<12} | {'Dup Rate':<8}")
    print("-" * 80)

    for filename in files:
        file_path = os.path.join(DATA_DIR, filename)
        file_rows = 0
        file_dups = 0
        file_new_unique = 0
        
        try:
            with open(file_path, 'r', encoding='utf-8', errors='replace') as f:
                reader = csv.reader(f)
                try:
                    header = next(reader) # Skip header
                except StopIteration:
                    continue # Empty file

                for row in reader:
                    if not row: continue
                    
                    # Assume ID is first column
                    try:
                        row_id = row[0]
                    except IndexError:
                        continue
                        
                    file_rows += 1
                    
                    if row_id in seen_ids:
                        file_dups += 1
                    else:
                        seen_ids.add(row_id)
                        file_new_unique += 1
            
            dup_rate = (file_dups / file_rows * 100) if file_rows > 0 else 0
            print(f"{filename:<30} | {file_rows:<12,} | {file_new_unique:<12,} | {file_dups:<12,} | {dup_rate:5.1f}%")
            
            total_stats["files_processed"] += 1
            total_stats["total_rows"] += file_rows
            total_stats["unique_rows"] += file_new_unique
            total_stats["duplicate_rows"] += file_dups

        except Exception as e:
            print(f"Error reading {filename}: {e}")

    print("-" * 80)
    total_dup_rate = (total_stats["duplicate_rows"] / total_stats["total_rows"] * 100) if total_stats["total_rows"] > 0 else 0
    print(f"{'TOTAL':<30} | {total_stats['total_rows']:<12,} | {len(seen_ids):<12,} | {total_stats['duplicate_rows']:<12,} | {total_dup_rate:5.1f}%")
    print("-" * 80)
    print(f"\n📈 Final Summary:")
    print(f"   Total Records Processed: {total_stats['total_rows']:,}")
    print(f"   Total Unique IDs:        {len(seen_ids):,}")
    print(f"   Total Duplicates:        {total_stats['duplicate_rows']:,}")
    print(f"   Overall Redundancy:      {total_dup_rate:.1f}%")

if __name__ == "__main__":
    analyze_duplicates()
