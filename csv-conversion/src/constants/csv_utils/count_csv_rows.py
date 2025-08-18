import sys
import csv

def count_rows_excluding_header(filename):
    """
    Returns the number of rows in a CSV file, excluding the header row.
    """
    with open(filename, newline='', encoding='shift-jis') as csvfile:
        reader = csv.reader(csvfile)
        next(reader, None)  # Skip header
        return sum(1 for _ in reader)


def main():
    if len(sys.argv) < 2:
        print("Usage: python count_csv_rows.py <csv_file>")
        return
    filename = sys.argv[1]
    num_rows = count_rows_excluding_header(filename)
    print(f"Number of rows in {filename}: {num_rows}")


if __name__ == "__main__":
    main()
