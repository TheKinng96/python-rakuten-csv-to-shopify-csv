import os
import csv

def split_csv_by_size(input_csv, max_bytes=10*1024*1024, output_prefix=None, encoding='utf-8'):
    if output_prefix is None:
        output_prefix = os.path.splitext(input_csv)[0] + '_part_'

    with open(input_csv, 'r', encoding=encoding, newline='') as infile:
        reader = csv.reader(infile)
        header = next(reader)
        part_num = 1
        out_file = f"{output_prefix}{part_num}.csv"
        outfile = open(out_file, 'w', encoding=encoding, newline='')
        writer = csv.writer(outfile)
        writer.writerow(header)
        current_size = outfile.tell()
        for row in reader:
            writer.writerow(row)
            # Check file size after writing
            if outfile.tell() >= max_bytes:
                outfile.close()
                print(f"Wrote {out_file} ({os.path.getsize(out_file)/1024/1024:.2f} MB)")
                part_num += 1
                out_file = f"{output_prefix}{part_num}.csv"
                outfile = open(out_file, 'w', encoding=encoding, newline='')
                writer = csv.writer(outfile)
                writer.writerow(header)
        outfile.close()
        print(f"Wrote {out_file} ({os.path.getsize(out_file)/1024/1024:.2f} MB)")

if __name__ == "__main__":
    # Try encodings for Japanese files
    for enc in ("cp932", "shift-jis", "utf-8"):
        try:
            split_csv_by_size("./sample/dl-normal-item_no_desc.csv", max_bytes=10*1024*1024, encoding=enc)
            break
        except UnicodeDecodeError:
            continue
