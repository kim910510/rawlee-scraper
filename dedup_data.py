
import csv
import sys
import shutil
from pathlib import Path

def deduplicate_csv(input_file: str, output_file: str):
    print(f"🧹 Deduplicating {input_file}...")
    
    seen_ids = set()
    duplicates = 0
    total_rows = 0
    written_rows = 0
    
    input_path = Path(input_file)
    output_path = Path(output_file)
    
    if not input_path.exists():
        print(f"❌ Input file not found: {input_file}")
        return

    with open(input_path, 'r', encoding='utf-8', errors='replace') as f_in, \
         open(output_path, 'w', encoding='utf-8') as f_out:
        
        reader = csv.reader(f_in)
        writer = csv.writer(f_out, quoting=csv.QUOTE_ALL)
        
        try:
            header = next(reader)
            writer.writerow(header)
        except StopIteration:
            print("⚠️ Empty file")
            return

        for row in reader:
            total_rows += 1
            if not row:
                continue
                
            # Assume ID is the first column
            try:
                row_id = row[0]
            except IndexError:
                continue
                
            if row_id in seen_ids:
                duplicates += 1
                continue
            
            seen_ids.add(row_id)
            writer.writerow(row)
            written_rows += 1
            
            if total_rows % 500000 == 0:
                print(f"   Processed {total_rows:,} rows... (Duplicates: {duplicates:,})")

    print(f"✅ Deduplication complete!")
    print(f"   Original rows: {total_rows:,}")
    print(f"   Unique rows:   {written_rows:,}")
    print(f"   Duplicates:    {duplicates:,} ({duplicates/total_rows*100:.1f}%)")
    
    # Backup original
    backup_path = input_path.with_name(input_path.name + ".bak")
    print(f"📦 Backing up original to {backup_path}")
    shutil.move(input_path, backup_path)
    
    # Rename output to original
    print(f"📄 Replacing original file")
    shutil.move(output_path, input_path)

if __name__ == "__main__":
    deduplicate_csv("data/products.csv", "data/products_clean.csv")
