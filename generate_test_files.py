import csv
import random
import sys
from datetime import datetime, timedelta
import ipaddress

# Define protocol numbers for flow logs
PROTOCOLS = {
    'tcp': '6',
    'udp': '17',
    'icmp': '1',
    'unknown': '999'
}

def generate_lookup_table(file_path, num_mappings=10000):
    """
    Generates the lookup_table.csv file, mapping (dstport, protocol) to tags.

    Args:
        file_path (str): The path to the CSV file to be generated.
        num_mappings (int): The number of mappings to generate.
    """
    protocols = ['tcp', 'udp', 'icmp']
    tags = [f"sv_P{i}" for i in range(1, 101)]  # Example tags sv_P1 to sv_P100

    lookup_data = []
    for _ in range(num_mappings):
        dstport = str(random.randint(1, 65535))
        protocol = random.choice(protocols)
        tag = random.choice(tags)
        lookup_data.append({
            'dstport': dstport,
            'protocol': protocol.lower(),  # Ensure lowercase for case-insensitivity
            'tag': tag.lower()             # Ensure lowercase for case-insensitivity
        })

    try:
        with open(file_path, mode='w', newline='', encoding='ascii') as csvfile:
            fieldnames = ['dstport', 'protocol', 'tag']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)

            writer.writeheader()
            for row in lookup_data:
                writer.writerow(row)
        print(f"Lookup table generated at '{file_path}' with {num_mappings} mappings.")
    except Exception as e:
        print(f"Error generating lookup table: {e}")
        sys.exit(1)

def random_ip():
    """
    Generates a random IPv4 address.

    Returns:
        str: A randomly generated IP address.
    """
    return str(ipaddress.IPv4Address(random.randint(0, (1 << 32) - 1)))

def generate_flow_log(file_path, lookup_file_path, num_entries=100000, include_malformed=False):
    """
    Generates the flow_log_file containing a specified number of log entries.

    Args:
        file_path (str): The path to the flow log file to be generated.
        lookup_file_path (str): The path to the lookup table CSV file.
        num_entries (int): The number of log entries to generate.
        include_malformed (bool): Whether to include malformed lines.
    """
    actions = ['allow', 'deny', 'drop']

    # Load lookup mappings for realistic data
    lookup_mappings = []
    try:
        with open(lookup_file_path, mode='r', encoding='ascii') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                lookup_mappings.append({
                    'dstport': row['dstport'].lower(),
                    'protocol': row['protocol'].lower(),
                    'tag': row['tag'].lower()
                })
    except Exception as e:
        print(f"Error reading lookup table: {e}")
        sys.exit(1)

    if not lookup_mappings:
        print("Lookup table is empty. Cannot generate flow log.")
        sys.exit(1)

    start_time = datetime.now()

    try:
        with open(file_path, 'w', encoding='ascii') as file:
            for i in range(num_entries):
                timestamp = (start_time + timedelta(seconds=i)).strftime('%Y-%m-%dT%H:%M:%SZ')
                src_ip = random_ip()
                src_port = random.randint(1024, 65535)
                dst_ip = random_ip()
                field5 = '0'

                # Select a mapping for dstport and protocol
                mapping = random.choice(lookup_mappings)
                dstport = mapping['dstport']
                protocol = mapping['protocol']
                protocol_num = PROTOCOLS.get(protocol, '999')
                action = random.choice(actions)
                values = [f"value{j}" for j in range(1, 7)]

                fields = [
                    timestamp,
                    src_ip,
                    str(src_port),
                    dst_ip,
                    field5,
                    dstport,
                    protocol_num,
                    action
                ] + values

                # Decide whether to insert a malformed line
                if include_malformed and random.random() < 0.05:
                    num_fields = random.randint(5, 13)  # Less than 14 fields
                    malformed_fields = fields[:num_fields]
                    line = ' '.join(malformed_fields) + '\n'
                else:
                    line = ' '.join(fields) + '\n'

                file.write(line)
        print(f"Flow log file generated at '{file_path}' with {num_entries} entries (~100MB).")
    except Exception as e:
        print(f"Error generating flow log file: {e}")
        sys.exit(1)

def main():
    """
    Main function to generate test files.
    """
    lookup_file = 'lookup_table.csv'
    flow_log_file = 'flow_log_file'
    num_mappings = 10000      # Up to 10,000 lookup mappings
    num_entries = 1000000      # Adjusted for up to ~100MB flow log file
    include_malformed = True  # Whether to include malformed lines to test parser robustness

    generate_lookup_table(lookup_file, num_mappings)
    generate_flow_log(flow_log_file, lookup_file, num_entries, include_malformed)

if __name__ == "__main__":
    main()