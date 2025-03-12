import boto3
import csv
from datetime import datetime, timedelta
import logging
from collections import defaultdict
from typing import Dict, List, Generator
import shutil
import math

# ANSI color codes
GREEN = '\033[0;32m'
RESET = '\033[0m'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def chunk_date_range(start_date: str, end_date: str, chunk_size: int = 30) -> Generator[tuple, None, None]:
    """Split date range into smaller chunks to avoid API limits."""
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')

    current = start
    while current < end:
        chunk_end = min(current + timedelta(days=chunk_size), end)
        yield current.strftime('%Y-%m-%d'), chunk_end.strftime('%Y-%m-%d')
        current = chunk_end


def get_cost_explorer_data_paginated(start_date: str, end_date: str, cluster_name: str) -> Generator[dict, None, None]:
    """Get cost data with pagination support."""
    ce = boto3.client('ce')

    for chunk_start, chunk_end in chunk_date_range(start_date, end_date):
        next_token = None
        while True:
            kwargs = {
                'TimePeriod': {
                    'Start': chunk_start,
                    'End': chunk_end
                },
                'Granularity': 'DAILY',
                'Metrics': ['UnblendedCost', 'UsageQuantity'],
                'GroupBy': [
                    {'Type': 'DIMENSION', 'Key': 'INSTANCE_TYPE'},
                    {'Type': 'DIMENSION', 'Key': 'PURCHASE_TYPE'}
                ],
                'Filter': {
                    'And': [
                        {
                            'Dimensions': {
                                'Key': 'SERVICE',
                                'Values': ['Amazon Elastic Compute Cloud - Compute']
                            }
                        },
                        {
                            'Tags': {
                                'Key': 'aws:eks:cluster-name',
                                'Values': [cluster_name]
                            }
                        }
                    ]
                }
            }

            if next_token:
                kwargs['NextPageToken'] = next_token

            response = ce.get_cost_and_usage(**kwargs)

            for result in response['ResultsByTime']:
                yield result

            next_token = response.get('NextPageToken')
            if not next_token:
                break


def write_csv_in_chunks(output_file: str, data_generator, instance_vcpu: dict, region: str):
    """Write CSV data in chunks to handle large datasets."""
    header = ['date', 'region', 'instance_type', 'purchase_type', 'number_of_cpus',
              'usage_hours', 'total_cost', 'cost_per_hour']

    row_count = 0
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)

        for result in data_generator:
            date = result['TimePeriod']['Start']
            date_str = datetime.strptime(date, '%Y-%m-%d').strftime('%Y-%m-%d')

            for group in result.get('Groups', []):
                instance_type, purchase_type = parse_keys(group['Keys'])
                usage_hours = float(group['Metrics']['UsageQuantity']['Amount'])
                total_cost = float(group['Metrics']['UnblendedCost']['Amount'])
                vcpus = instance_vcpu.get(instance_type, 1)
                cost_per_hour = total_cost / usage_hours if usage_hours > 0 else 0

                writer.writerow([
                    date_str,
                    region,
                    instance_type,
                    purchase_type,
                    vcpus,
                    usage_hours,
                    total_cost,
                    cost_per_hour
                ])
                row_count += 1

                if row_count % 1000 == 0:
                    logging.info(f"Processed {row_count} rows...")

    return row_count

def find_earliest_available_date():
    """Find the earliest date with available cost data using binary search"""
    ce = boto3.client('ce')
    end_date = datetime.now()
    # Start with minimum possible - 14 months ago
    min_start = end_date - timedelta(days=14 * 30)
    max_start = end_date
    earliest_valid_date = end_date

    logging.info("Searching for the earliest available date...")

    while (max_start - min_start).days > 1:
        mid_date = min_start + (max_start - min_start) / 2
        test_date = mid_date.strftime('%Y-%m-%d')

        try:
            response = ce.get_cost_and_usage(
                TimePeriod={
                    'Start': test_date,
                    'End': end_date.strftime('%Y-%m-%d')
                },
                Granularity='MONTHLY',
                Metrics=['UnblendedCost']
            )
            # If we got a valid response, try an earlier date
            max_start = mid_date
            earliest_valid_date = mid_date
        except Exception as e:
            # If we got an error, try a later date
            min_start = mid_date

    logging.info(f"Earliest available date found: {earliest_valid_date.strftime('%Y-%m-%d')}")
    return earliest_valid_date.strftime('%Y-%m-%d')


def get_date_range() -> tuple:
    """Get optimal date range based on available data"""
    end_date = datetime.now().strftime('%Y-%m-%d')

    # Find earliest available date
    earliest_date = find_earliest_available_date()

    # Calculate start date as the later of:
    # 1. The earliest available date
    # 2. 12 months ago
    twelve_months_ago = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    start_date = max(earliest_date, twelve_months_ago)

    logging.info(f"Using date range: {start_date} to {end_date}")
    return start_date, end_date

def get_terminal_size():
    """Get terminal width, default to 80 if can't determine"""
    try:
        terminal_width, _ = shutil.get_terminal_size()
        return max(80, terminal_width)
    except:
        return 80

def format_columns(items, width, start_idx):
    """Format items in columns that fit within the specified width"""
    if not items:
        return []

    # Calculate the maximum length of an item plus its number
    max_item_length = max(len(f"{idx}. {item}") for idx, item in enumerate(items, start_idx))

    # Add padding between columns
    column_width = max_item_length + 2

    # Calculate number of columns that can fit
    num_columns = max(1, width // column_width)

    # Calculate number of rows needed
    num_rows = math.ceil(len(items) / num_columns)

    # Create the row-based layout
    rows = []
    for row_idx in range(num_rows):
        row = []
        for col_idx in range(num_columns):
            item_idx = col_idx * num_rows + row_idx
            if item_idx < len(items):
                item = items[item_idx]
                # Format the item with its number and padding
                formatted_item = f"{item_idx + start_idx}. {item}".ljust(column_width)
                row.append(formatted_item)
        rows.append("".join(row).rstrip())

    return rows

def get_clusters_from_billing(start_date: str, end_date: str) -> Dict[str, List[str]]:
    """Fetch all cluster names that exist in billing history grouped by region."""
    ce = boto3.client('ce')

    # Get data for the last year to catch all clusters
    # end_date = datetime.now().strftime('%Y-%m-%d')
    # start_date = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')

    try:
        response = ce.get_cost_and_usage(
            TimePeriod={
                'Start': start_date,
                'End': end_date
            },
            Granularity='MONTHLY',
            Metrics=['UnblendedCost'],
            GroupBy=[
                {'Type': 'TAG', 'Key': 'aws:eks:cluster-name'},
                {'Type': 'DIMENSION', 'Key': 'REGION'}
            ],
            Filter={
                'Dimensions': {
                    'Key': 'SERVICE',
                    'Values': ['Amazon Elastic Compute Cloud - Compute']
                }
            }
        )

        clusters_by_region = defaultdict(set)

        for result in response['ResultsByTime']:
            for group in result.get('Groups', []):
                # Extract cluster name and region from the group
                cluster_tag = group['Keys'][0]
                region = group['Keys'][1]

                # Skip empty or invalid cluster names
                if cluster_tag and not cluster_tag.endswith('$'):
                    cluster_name = cluster_tag.split('$')[-1]
                    if cluster_name:
                        clusters_by_region[region].add(cluster_name)

        # Convert sets to sorted lists and create final dictionary
        return {region: sorted(list(clusters))
                for region, clusters in sorted(clusters_by_region.items())
                if clusters}  # Only include regions with clusters

    except Exception as e:
        logging.error(f"Error fetching clusters from billing: {str(e)}")
        return {}

def select_cluster(clusters_by_region: Dict[str, List[str]]) -> tuple:
    """Interactive cluster selection with region grouping and columnar display."""
    if not clusters_by_region:
        logging.error("No EKS clusters found in billing history")
        exit(1)

    # Create a flat list for selection while maintaining region information
    selection_map = []

    # Get terminal width for formatting
    terminal_width = get_terminal_size()

    print("\nAvailable EKS clusters from billing history by region:")
    print("===================================================")

    current_idx = 1  # Keep track of the current cluster number

    for region, clusters in clusters_by_region.items():
        # Print region name in green and uppercase with extra line break
        print(f"\n{GREEN}{region.upper()}{RESET}:\n")

        # Create list of clusters for this region
        region_clusters = []
        for cluster in clusters:
            selection_map.append((region, cluster))
            region_clusters.append(cluster)

        # Format and print clusters in columns
        formatted_rows = format_columns(region_clusters, terminal_width, current_idx)
        for row in formatted_rows:
            print(row)

        # Update the index for the next region
        current_idx += len(clusters)

    while True:
        try:
            choice = input("\nSelect cluster number (or 'q' to quit): ")

            if choice.lower() == 'q':
                logging.info("Exiting program")
                exit(0)

            choice_idx = int(choice) - 1
            if 0 <= choice_idx < len(selection_map):
                selected_region, selected_cluster = selection_map[choice_idx]
                return selected_region, selected_cluster
            else:
                print(f"Please enter a number between 1 and {len(selection_map)}")
        except ValueError:
            print("Please enter a valid number")

def get_ec2_instance_types():
    ec2 = boto3.client('ec2')
    paginator = ec2.get_paginator('describe_instance_types')
    instance_types = {}
    for page in paginator.paginate():
        for it in page['InstanceTypes']:
            instance_types[it['InstanceType']] = it['VCpuInfo']['DefaultVCpus']
    return instance_types

def get_cost_explorer_data(start_date, end_date, cluster_name):
    ce = boto3.client('ce')

    response = ce.get_cost_and_usage(
        TimePeriod={
            'Start': start_date,
            'End': end_date
        },
        Granularity='DAILY',
        Metrics=['UnblendedCost', 'UsageQuantity'],
        GroupBy=[
            {'Type': 'DIMENSION', 'Key': 'INSTANCE_TYPE'},
            {'Type': 'DIMENSION', 'Key': 'PURCHASE_TYPE'}
        ],
        Filter={
            'And': [
                {
                    'Dimensions': {
                        'Key': 'SERVICE',
                        'Values': ['Amazon Elastic Compute Cloud - Compute']
                    }
                },
                {
                    'Tags': {
                        'Key': 'aws:eks:cluster-name',
                        'Values': [cluster_name]
                    }
                }
            ]
        }
    )

    return response['ResultsByTime']

def parse_keys(keys):
    instance_type = keys[0] if len(keys) > 0 else 'Unknown'
    purchase_type = keys[1] if len(keys) > 1 else 'Unknown'

    if purchase_type.endswith('Instances'):
        purchase_type = purchase_type.replace(' Instances', '')

    return instance_type, purchase_type


def main():
    start_date, end_date = get_date_range()
    clusters_by_region = get_clusters_from_billing(start_date, end_date)
    region, cluster_name = select_cluster(clusters_by_region)

    logging.info(f"Selected cluster: {cluster_name} in region: {region}")
    logging.info(f"Fetching EC2 compute data for cluster: {cluster_name}")
    logging.info(f"Date range: {start_date} to {end_date}")

    instance_vcpu = get_ec2_instance_types()
    cost_data_generator = get_cost_explorer_data_paginated(start_date, end_date, cluster_name)

    output_file = f'aws_ec2_cost_report_{cluster_name}_{region}.csv'
    total_rows = write_csv_in_chunks(output_file, cost_data_generator, instance_vcpu, region)

    logging.info(f"CSV report generated: {output_file}")
    logging.info(f"Total rows in the report: {total_rows}")


if __name__ == "__main__":
    main()