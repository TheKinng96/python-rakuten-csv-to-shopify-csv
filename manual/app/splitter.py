import csv
import os

def split_csv(path, chunk_size=1024*1024):  # Default chunk size: 1MB
    """
    Split a large CSV file into smaller chunks.
    
    Args:
        path (str): Path to the input CSV file
        chunk_size (int): Size of each chunk in bytes
    """
    try:
        with open(path, 'r', encoding='utf-8') as infile:
            reader = csv.reader(infile)
            header = next(reader)  # Get header
            
            part_num = 1
            current_size = 0
            current_chunk = []
            
            for row in reader:
                row_size = len(''.join(row).encode('utf-8'))
                
                if current_size + row_size > chunk_size and current_chunk:
                    # Write current chunk
                    write_chunk(header, current_chunk, part_num)
                    current_chunk = []
                    current_size = 0
                    part_num += 1
                
                current_chunk.append(row)
                current_size += row_size
            
            # Write remaining rows
            if current_chunk:
                write_chunk(header, current_chunk, part_num)
                
    except Exception as e:
        raise Exception(f"Error splitting CSV: {str(e)}")

def write_chunk(header, rows, part_num):
    """
    Write a chunk of rows to a new CSV file.
    
    Args:
        header (list): CSV header row
        rows (list): List of rows to write
        part_num (int): Part number for filename
    """
    filename = f"file_part{part_num}.csv"
    with open(filename, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.writer(outfile)
        writer.writerow(header)
        writer.writerows(rows)
