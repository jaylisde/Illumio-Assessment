import csv
import sys
import os
from collections import defaultdict
from multiprocessing import Pool, Manager, cpu_count
import itertools
import time

# Mapping of numerical protocol identifiers to protocol names
PROTOCOL_MAP = {
    '6': 'tcp',
    '17': 'udp',
    '1': 'icmp'
}

def load_lookup_table(lookup_file):
    """
    Loads the lookup table from a CSV file.

    Args:
        lookup_file (str): Path to the lookup CSV file.

    Returns:
        dict: A dictionary with (dstport, protocol) as keys and tags as values.
    """
    lookup = {}
    try:
        with open(lookup_file, mode='r', newline='', encoding='ascii') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                dstport = row['dstport'].strip().lower()
                protocol = row['protocol'].strip().lower()
                tag = row['tag'].strip()
                lookup[(dstport, protocol)] = tag
    except Exception as e:
        print(f"Error reading lookup table file: {e}")
        sys.exit(1)
    return lookup

def map_protocol(protocol_num):
    """
    Maps numerical protocol identifiers to protocol names.

    Args:
        protocol_num (str): Numerical protocol identifier.

    Returns:
        str: Protocol name.
    """
    return PROTOCOL_MAP.get(protocol_num, 'unknown')

def process_chunk(chunk, lookup_dict):
    """
    Processes a chunk of the flow log file.

    Args:
        chunk (list): List of lines in the chunk.
        lookup_dict (dict): Lookup dictionary for tag mapping.

    Returns:
        tuple: Two defaultdicts containing tag counts and port/protocol counts.
    """
    # print(f"Processing chunk {chunk[0][0]} to {chunk[-1][0]}")
    tag_counts = defaultdict(int)
    port_protocol_counts = defaultdict(int)

    for line_num, line in chunk:
        parts = line.strip().split()
        if len(parts) < 14:
            # Skipping malformed lines in parallel processing
            continue

        dstport = parts[5].strip().lower()
        protocol_num = parts[6].strip()
        protocol = map_protocol(protocol_num).lower()

        # Update port/protocol counts
        port_protocol_key = (dstport, protocol)
        port_protocol_counts[port_protocol_key] += 1

        # Get tag from lookup
        tag = lookup_dict.get((dstport, protocol))
        if tag:
            tag_counts[tag] += 1
        else:
            tag_counts['Untagged'] += 1

    return tag_counts, port_protocol_counts

def write_output(output_file, total_tag_counts, total_port_protocol_counts):
    """
    Writes the tag counts and port/protocol combination counts to the output file.

    Args:
        output_file (str): Path to the output file.
        total_tag_counts (dict): Dictionary of total tag counts.
        total_port_protocol_counts (dict): Dictionary of total port/protocol counts.
    """
    try:
        with open(output_file, 'w', encoding='ascii') as file:
            # Write Tag Counts
            file.write("Tag Counts:\n")
            file.write("Tag,Count\n")
            for tag, count in sorted(total_tag_counts.items()):
                file.write(f"{tag},{count}\n")

            file.write("\nPort/Protocol Combination Counts:\n")
            file.write("Port,Protocol,Count\n")
            for (port, protocol), count in sorted(total_port_protocol_counts.items()):
                file.write(f"{port},{protocol},{count}\n")
    except Exception as e:
        print(f"Error writing to output file: {e}")
        sys.exit(1)

def chunked_file_reader(file_path, chunk_size=100000):
    """
    Generator that reads a file and yields chunks of lines.

    Args:
        file_path (str): Path to the file to read.
        chunk_size (int, optional): Number of lines per chunk. Defaults to 100000.

    Yields:
        list: List of tuples containing line number and line content.
    """
    with open(file_path, 'r', encoding='ascii') as file:
        chunk = []
        for line_num, line in enumerate(file, 1):
            chunk.append((line_num, line))
            if len(chunk) == chunk_size:
                yield chunk
                chunk = []
        if chunk:
            yield chunk

def aggregate_counts(total, partial):
    """
    Aggregates partial counts into the total counts.

    Args:
        total (defaultdict): The total counts.
        partial (tuple): A tuple containing partial tag_counts and port_protocol_counts.

    Returns:
        None
    """
    tag_counts, port_protocol_counts = partial
    for tag, count in tag_counts.items():
        total[tag] += count
    for key, count in port_protocol_counts.items():
        total[key] += count

def main():
    """
    Main function to execute the optimized flow log parser.
    """
    if len(sys.argv) != 4:
        print("Usage: python flow_log_parser.py <flow_log_file> <lookup_csv_file> <output_file>")
        sys.exit(1)

    flow_log_file = sys.argv[1]
    lookup_csv_file = sys.argv[2]
    output_file = sys.argv[3]

    # Check if input files exist
    if not os.path.isfile(flow_log_file):
        print(f"Error: Flow log file '{flow_log_file}' does not exist.")
        sys.exit(1)
    if not os.path.isfile(lookup_csv_file):
        print(f"Error: Lookup table file '{lookup_csv_file}' does not exist.")
        sys.exit(1)

    # Load lookup table
    lookup_dict = load_lookup_table(lookup_csv_file)

    # Initialize total counts
    total_tag_counts = defaultdict(int)
    total_port_protocol_counts = defaultdict(int)

    # Determine the number of processes to use
    num_processes = cpu_count()

    start_time = time.time()

    with Pool(processes=num_processes) as pool:
        results = []
        for chunk in chunked_file_reader(flow_log_file):
            result = pool.apply_async(process_chunk, args=(chunk, lookup_dict))
            results.append(result)

        # Aggregate results as they complete
        for result in results:
            tag_counts, port_protocol_counts = result.get()
            for tag, count in tag_counts.items():
                total_tag_counts[tag] += count
            for key, count in port_protocol_counts.items():
                total_port_protocol_counts[key] += count

    # Convert managed dicts to regular dicts for writing
    total_tag_counts = dict(total_tag_counts)
    total_port_protocol_counts = dict(total_port_protocol_counts)

    # Write output
    write_output(output_file, total_tag_counts, total_port_protocol_counts)

    end_time = time.time()
    elapsed_time = end_time - start_time
    print(f"Processing complete. Output written to '{output_file}'. Time taken: {elapsed_time:.2f} seconds.")

if __name__ == "__main__":
    main() 