
import csv
import os
import shutil
from pathlib import Path

DATA_DIR = "/Users/a1234/Downloads/project-Whick/data"
OUTPUT_FILE = os.path.join(DATA_DIR, "products_merged.csv")
SEEN_IDS_FILE = "/Users/a1234/Downloads/project-Whick/shop/crawlee_scraper/data/.seen_ids"

def deduplicate_all():
    print(f"🧹 Deduplicating ALL files in {DATA_DIR}...")
    
    files = [f for f in os.listdir(DATA_DIR) if f.startswith('products') and f.endswith('.csv') and 'merged' not in f and 'clean' not in f]
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

    print(f"📦 Output file: {OUTPUT_FILE}")
    
    with open(OUTPUT_FILE, 'w', encoding='utf-8', newline='') as f_out, \
         open(SEEN_IDS_FILE, 'w', encoding='utf-8') as f_seen:
        
        writer = csv.writer(f_out, quoting=csv.QUOTE_ALL)
        header_written = False
        
        for filename in files:
            file_path = os.path.join(DATA_DIR, filename)
            print(f"📄 Processing {filename}...", end="", flush=True)
            
            file_rows = 0
            file_dups = 0
            
            try:
                with open(file_path, 'r', encoding='utf-8', errors='replace') as f_in:
                    reader = csv.reader(f_in)
                    try:
                        header = next(reader)
                        if not header_written:
                            writer.writerow(header)
                            header_written = True
                    except StopIteration:
                        print(" (Empty)")
                        continue

                    for row in reader:
                        if not row: continue
                        
                        try:
                            row_id = row[0]
                        except IndexError:
                            continue
                            
                        file_rows += 1
                        
                        if row_id in seen_ids:
                            file_dups += 1
                        else:
                            seen_ids.add(row_id)
                            writer.writerow(row)
                            f_seen.write(f"{row_id}\n")
                            total_stats["unique_rows"] += 1
            except Exception as e:
                print(f" Error reading file: {e}")
                continue
            
            unique_count = file_rows - file_dups
            print(f" Done! (+{unique_count:,} unique)")
            total_stats["files_processed"] += 1
            total_stats["total_rows"] += file_rows
            total_stats["duplicate_rows"] += file_dups

    print("\n✅ Deduplication Complete!")
    print(f"   Files Processed: {total_stats['files_processed']}")
    print(f"   Total Rows Read: {total_stats['total_rows']:,}")
    print(f"   Unique Rows:     {total_stats['unique_rows']:,}")
    print(f"   Duplicates:      {total_stats['duplicate_rows']:,}")
    
    # Compress original files? No, just keep them for now or let user decide
    print(f"\n📁 Unique IDs saved to: {SEEN_IDS_FILE}")
    print(f"💾 Clean data saved to: {OUTPUT_FILE}")

if __name__ == "__main__":
    deduplicate_all()
