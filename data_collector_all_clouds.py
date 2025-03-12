import boto3
import csv
from datetime import datetime, timedelta
import logging
from collections import defaultdict
from typing import Dict, List, Generator, Tuple, Optional
import shutil
import math
import os
import json
import argparse
from google.cloud import billing
from google.cloud import bigquery
from azure.identity import DefaultAzureCredential
from azure.mgmt.consumption import ConsumptionManagementClient
from azure.mgmt.resource import ResourceManagementClient

# ANSI color codes
GREEN = '\033[0;32m'
YELLOW = '\033[0;33m'
BLUE = '\033[0;34m'
RESET = '\033[0m'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CloudProvider:
    AWS = "aws"
    GCP = "gcp"
    AZURE = "azure"

    @staticmethod
    def all():
        return [CloudProvider.AWS, CloudProvider.GCP, CloudProvider.AZURE]

    @staticmethod
    def validate(provider):
        if provider.lower() not in CloudProvider.all():
            raise ValueError(f"Invalid cloud provider. Choose from: {', '.join(CloudProvider.all())}")
        return provider.lower()

# =====================================================================
# Common Utilities
# =====================================================================

def chunk_date_range(start_date: str, end_date: str, chunk_size: int = 30) -> Generator[tuple, None, None]:
    """Split date range into smaller chunks to avoid API limits."""
    start = datetime.strptime(start_date, '%Y-%m-%d')
    end = datetime.strptime(end_date, '%Y-%m-%d')

    current = start
    while current < end:
        chunk_end = min(current + timedelta(days=chunk_size), end)
        yield current.strftime('%Y-%m-%d'), chunk_end.strftime('%Y-%m-%d')
        current = chunk_end

def get_date_range() -> tuple:
    """Get optimal date range based on available data"""
    end_date = datetime.now().strftime('%Y-%m-%d')
    # Use 12 months as default
    twelve_months_ago = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    return twelve_months_ago, end_date

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

def write_csv_in_chunks(output_file: str, data_generator, headers: List[str]):
    """Write CSV data in chunks to handle large datasets."""
    row_count = 0
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(headers)

        for row in data_generator:
            writer.writerow(row)
            row_count += 1

            if row_count % 1000 == 0:
                logging.info(f"Processed {row_count} rows...")

    return row_count

# =====================================================================
# AWS Implementation
# =====================================================================

def aws_find_earliest_available_date():
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

def aws_get_date_range() -> tuple:
    """Get optimal date range based on available AWS data"""
    end_date = datetime.now().strftime('%Y-%m-%d')

    # Find earliest available date
    earliest_date = aws_find_earliest_available_date()

    # Calculate start date as the later of:
    # 1. The earliest available date
    # 2. 12 months ago
    twelve_months_ago = (datetime.now() - timedelta(days=365)).strftime('%Y-%m-%d')
    start_date = max(earliest_date, twelve_months_ago)

    logging.info(f"Using date range: {start_date} to {end_date}")
    return start_date, end_date

def aws_get_clusters_from_billing(start_date: str, end_date: str) -> Dict[str, List[str]]:
    """Fetch all cluster names that exist in billing history grouped by region."""
    ce = boto3.client('ce')

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

def aws_get_ec2_instance_types():
    """Get EC2 instance types with vCPU counts"""
    ec2 = boto3.client('ec2')
    paginator = ec2.get_paginator('describe_instance_types')
    instance_types = {}
    for page in paginator.paginate():
        for it in page['InstanceTypes']:
            instance_types[it['InstanceType']] = it['VCpuInfo']['DefaultVCpus']
    return instance_types

def aws_get_cost_explorer_data_paginated(start_date: str, end_date: str, cluster_name: str) -> Generator[dict, None, None]:
    """Get AWS cost data with pagination support."""
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

def aws_parse_keys(keys):
    """Parse AWS keys for instance type and purchase type"""
    instance_type = keys[0] if len(keys) > 0 else 'Unknown'
    purchase_type = keys[1] if len(keys) > 1 else 'Unknown'

    if purchase_type.endswith('Instances'):
        purchase_type = purchase_type.replace(' Instances', '')

    return instance_type, purchase_type

def aws_process_cost_data(cost_data_generator, instance_vcpu, region):
    """Process AWS cost data into rows for CSV"""
    for result in cost_data_generator:
        date = result['TimePeriod']['Start']
        date_str = datetime.strptime(date, '%Y-%m-%d').strftime('%Y-%m-%d')

        for group in result.get('Groups', []):
            instance_type, purchase_type = aws_parse_keys(group['Keys'])
            usage_hours = float(group['Metrics']['UsageQuantity']['Amount'])
            total_cost = float(group['Metrics']['UnblendedCost']['Amount'])
            vcpus = instance_vcpu.get(instance_type, 1)
            cost_per_hour = total_cost / usage_hours if usage_hours > 0 else 0

            yield [
                date_str,
                region,
                instance_type,
                purchase_type,
                vcpus,
                usage_hours,
                total_cost,
                cost_per_hour,
                'AWS'  # Add cloud provider column
            ]

def run_aws_report(start_date, end_date):
    """Run AWS cost report workflow"""
    # Use AWS-specific date range if needed
    start_date, end_date = aws_get_date_range()
    
    clusters_by_region = aws_get_clusters_from_billing(start_date, end_date)
    
    if not clusters_by_region:
        logging.error("No EKS clusters found in AWS billing history")
        return
        
    region, cluster_name = select_resource("AWS EKS Clusters", clusters_by_region)

    logging.info(f"Selected cluster: {cluster_name} in region: {region}")
    logging.info(f"Fetching EC2 compute data for cluster: {cluster_name}")
    logging.info(f"Date range: {start_date} to {end_date}")

    instance_vcpu = aws_get_ec2_instance_types()
    cost_data_generator = aws_get_cost_explorer_data_paginated(start_date, end_date, cluster_name)
    
    # Process the data
    processed_data = aws_process_cost_data(cost_data_generator, instance_vcpu, region)

    # Define CSV headers
    headers = ['date', 'region', 'instance_type', 'purchase_type', 'number_of_cpus',
               'usage_hours', 'total_cost', 'cost_per_hour', 'cloud_provider']

    # Write to CSV
    output_file = f'cloud_cost_report_aws_{cluster_name}_{region}.csv'
    total_rows = write_csv_in_chunks(output_file, processed_data, headers)

    logging.info(f"AWS CSV report generated: {output_file}")
    logging.info(f"Total rows in the report: {total_rows}")

# =====================================================================
# GCP Implementation
# =====================================================================

def gcp_get_billing_accounts():
    """Get available GCP billing accounts"""
    try:
        client = billing.CloudBillingClient()
        billing_accounts = client.list_billing_accounts()
        
        accounts = []
        for account in billing_accounts:
            if account.open:  # Only include open billing accounts
                accounts.append({
                    "id": account.name.split('/')[-1],
                    "display_name": account.display_name
                })
        
        return accounts
    except Exception as e:
        logging.error(f"Error fetching GCP billing accounts: {str(e)}")
        return []

def gcp_get_projects(billing_account_id):
    """Get GCP projects linked to a billing account"""
    try:
        client = billing.CloudBillingClient()
        
        # Format the billing account name
        billing_account_name = f"billingAccounts/{billing_account_id}"
        
        # List projects associated with this billing account
        projects = client.list_project_billing_info(name=billing_account_name)
        
        # Organize projects by region or zone (approximated)
        projects_by_region = defaultdict(list)
        
        for project_billing_info in projects:
            if project_billing_info.project_id:
                # For GCP, we'll initially group by "global" since GCP's 
                # resource hierarchy doesn't map directly to regions
                projects_by_region["global"].append(project_billing_info.project_id)
        
        return {region: sorted(projects) for region, projects in projects_by_region.items()}
    except Exception as e:
        logging.error(f"Error fetching GCP projects: {str(e)}")
        return {}

def gcp_get_gke_clusters(project_id):
    """Get GKE clusters for a project"""
    # This is a placeholder - would use the actual GCP API for container
    try:
        from google.cloud import container_v1
        client = container_v1.ClusterManagerClient()
        parent = f"projects/{project_id}/locations/-"
        
        clusters_by_region = defaultdict(list)
        clusters_response = client.list_clusters(parent=parent)
        
        for cluster in clusters_response.clusters:
            location = cluster.location  # This might be a zone or region
            clusters_by_region[location].append(cluster.name)
        
        return {region: sorted(clusters) for region, clusters in clusters_by_region.items()}
    except Exception as e:
        logging.error(f"Error fetching GKE clusters: {str(e)}")
        return {"error": ["GKE API access failed"]}

def gcp_fetch_compute_data(project_id, start_date, end_date, cluster_name=None):
    """Fetch GCP compute engine costs from BigQuery"""
    try:
        # Initialize BigQuery client
        client = bigquery.Client()
        
        # Convert dates to YYYY-MM-DD format
        start_date_str = datetime.strptime(start_date, '%Y-%m-%d').strftime('%Y-%m-%d')
        end_date_str = datetime.strptime(end_date, '%Y-%m-%d').strftime('%Y-%m-%d')
        
        # Build query - filter for Compute Engine costs
        # Adding cluster filter if provided (using labels)
        cluster_filter = f"AND labels.key = 'goog-gke-cluster' AND labels.value = '{cluster_name}'" if cluster_name else ""
        
        query = f"""
        SELECT
          invoice.month as invoice_month,
          DATE(_PARTITIONTIME) as usage_date,
          location.region as region,
          sku.description as machine_type,
          "Regular" as purchase_type,  -- Default to Regular, could determine Reserved or Spot if needed
          CAST(system_labels[OFFSET(0)].value AS INT64) as vcpu_count,  -- Adjust based on your data structure
          SUM(usage.amount) as usage_amount,
          SUM(cost) as total_cost,
          SUM(cost) / NULLIF(SUM(usage.amount), 0) as cost_per_unit
        FROM
          `{project_id}.billing_export_dataset.gcp_billing_export_v1_*`
        WHERE
          DATE(_PARTITIONTIME) BETWEEN '{start_date_str}' AND '{end_date_str}'
          AND service.description = 'Compute Engine'
          {cluster_filter}
        GROUP BY
          invoice_month, usage_date, region, machine_type, purchase_type, vcpu_count
        ORDER BY
          usage_date
        """
        
        # Run the query
        query_job = client.query(query)
        
        # Process results
        for row in query_job:
            yield [
                row.usage_date.strftime('%Y-%m-%d'),
                row.region or 'global',
                row.machine_type,
                row.purchase_type,
                row.vcpu_count or 1,
                row.usage_amount,
                row.total_cost,
                row.cost_per_unit,
                'GCP'  # Add cloud provider column
            ]
            
    except Exception as e:
        logging.error(f"Error fetching GCP compute data: {str(e)}")
        # Yield an error record
        yield ["ERROR", "ERROR", f"Error: {str(e)}", "", "", "", "", "", "GCP"] 

def run_gcp_report(start_date, end_date):
    """Run GCP cost report workflow"""
    logging.info("Fetching GCP billing accounts...")
    
    billing_accounts = gcp_get_billing_accounts()
    
    if not billing_accounts:
        logging.error("No GCP billing accounts found or billing API not accessible")
        print("Please ensure you have the necessary GCP permissions and the Billing API is enabled")
        return
    
    # Select billing account
    print("\nAvailable GCP Billing Accounts:")
    for idx, account in enumerate(billing_accounts, 1):
        print(f"{idx}. {account['display_name']} ({account['id']})")
    
    while True:
        try:
            choice = input("\nSelect billing account number (or 'q' to quit): ")
            if choice.lower() == 'q':
                return
                
            choice_idx = int(choice) - 1
            if 0 <= choice_idx < len(billing_accounts):
                billing_account_id = billing_accounts[choice_idx]['id']
                break
            else:
                print(f"Please enter a number between 1 and {len(billing_accounts)}")
        except ValueError:
            print("Please enter a valid number")
    
    # Get projects for the selected billing account
    projects_by_region = gcp_get_projects(billing_account_id)
    
    if not projects_by_region:
        logging.error("No projects found for the selected billing account")
        return
    
    # Select project
    region, project_id = select_resource("GCP Projects", projects_by_region)
    
    # Choose between project-wide or cluster-specific report
    use_cluster = input("\nDo you want to filter by GKE cluster? (y/n): ").lower() == 'y'
    
    cluster_name = None
    if use_cluster:
        # Get clusters for the selected project
        clusters_by_region = gcp_get_gke_clusters(project_id)
        
        if clusters_by_region and "error" not in clusters_by_region:
            _, cluster_name = select_resource("GKE Clusters", clusters_by_region)
        else:
            logging.error("Unable to fetch GKE clusters or none found")
            print("Proceeding with project-wide report")
    
    # Process data
    logging.info(f"Fetching GCP compute data for project: {project_id}")
    if cluster_name:
        logging.info(f"Filtering by cluster: {cluster_name}")
    logging.info(f"Date range: {start_date} to {end_date}")
    
    # Get the data
    cost_data = gcp_fetch_compute_data(project_id, start_date, end_date, cluster_name)
    
    # CSV headers
    headers = ['date', 'region', 'machine_type', 'purchase_type', 'number_of_cpus',
               'usage_amount', 'total_cost', 'cost_per_unit', 'cloud_provider']
    
    # Determine output filename
    if cluster_name:
        output_file = f'cloud_cost_report_gcp_{project_id}_{cluster_name}.csv'
    else:
        output_file = f'cloud_cost_report_gcp_{project_id}.csv'
    
    # Write to CSV
    total_rows = write_csv_in_chunks(output_file, cost_data, headers)
    
    logging.info(f"GCP CSV report generated: {output_file}")
    logging.info(f"Total rows in the report: {total_rows}")

# =====================================================================
# Azure Implementation
# =====================================================================

def azure_get_subscriptions():
    """Get available Azure subscriptions"""
    try:
        # Initialize the credential
        credential = DefaultAzureCredential()
        
        # Initialize the Resource Management client
        resource_client = ResourceManagementClient(credential, subscription_id=None)
        
        # List subscriptions
        subscriptions = list(resource_client.subscriptions.list())
        
        # Convert to dictionary for easy selection
        subscription_dict = {}
        for sub in subscriptions:
            if sub.state == "Enabled":
                subscription_dict[sub.subscription_id] = sub.display_name
        
        return subscription_dict
    except Exception as e:
        logging.error(f"Error fetching Azure subscriptions: {str(e)}")
        return {}

def azure_get_resource_groups(subscription_id):
    """Get Azure resource groups by region"""
    try:
        # Initialize the credential
        credential = DefaultAzureCredential()
        
        # Initialize the Resource Management client
        resource_client = ResourceManagementClient(credential, subscription_id)
        
        # List resource groups
        resource_groups = resource_client.resource_groups.list()
        
        # Organize by region
        groups_by_region = defaultdict(list)
        for group in resource_groups:
            groups_by_region[group.location].append(group.name)
        
        return {region: sorted(groups) for region, groups in groups_by_region.items()}
    except Exception as e:
        logging.error(f"Error fetching Azure resource groups: {str(e)}")
        return {}

def azure_get_aks_clusters(subscription_id, resource_group=None):
    """Get AKS clusters in a subscription, optionally filtered by resource group"""
    try:
        from azure.mgmt.containerservice import ContainerServiceClient
        
        # Initialize the credential
        credential = DefaultAzureCredential()
        
        # Initialize the Container Service client
        container_client = ContainerServiceClient(credential, subscription_id)
        
        # List managed clusters
        clusters_by_region = defaultdict(list)
        
        if resource_group:
            # List clusters in specific resource group
            clusters = container_client.managed_clusters.list_by_resource_group(resource_group)
            for cluster in clusters:
                clusters_by_region[cluster.location].append(cluster.name)
        else:
            # List all clusters in subscription
            clusters = container_client.managed_clusters.list()
            for cluster in clusters:
                clusters_by_region[cluster.location].append(cluster.name)
        
        return {region: sorted(clusters) for region, clusters in clusters_by_region.items()}
    except Exception as e:
        logging.error(f"Error fetching AKS clusters: {str(e)}")
        return {}

def azure_fetch_compute_costs(subscription_id, start_date, end_date, resource_group=None, cluster_name=None):
    """Fetch Azure compute costs from the Consumption API"""
    try:
        # Initialize credential
        credential = DefaultAzureCredential()
        
        # Initialize the Consumption client
        consumption_client = ConsumptionManagementClient(credential, subscription_id)
        
        # Format dates
        start_date_obj = datetime.strptime(start_date, '%Y-%m-%d')
        end_date_obj = datetime.strptime(end_date, '%Y-%m-%d')
        
        # Build filter for consumption data
        # Filter for VM costs
        filter_str = "properties/meterCategory eq 'Virtual Machines'"
        
        # Add resource group filter if provided
        if resource_group:
            filter_str += f" and properties/resourceGroup eq '{resource_group}'"
            
        # Get usage details
        usage_details = consumption_client.usage_details.list(
            scope=f"/subscriptions/{subscription_id}",
            filter=filter_str,
            expand="properties/additionalProperties"
        )
        
        # Process and yield results
        for detail in usage_details:
            usage_date = detail.properties.usage_start
            
            # Skip records outside our date range
            if not (start_date_obj <= usage_date.date() <= end_date_obj.date()):
                continue
                
            # If filtering by cluster, check tags
            if cluster_name:
                # Azure AKS resources typically have tags
                tags = detail.properties.additional_properties.get("tags", {})
                if "kubernetes.io-cluster-name" not in tags or tags["kubernetes.io-cluster-name"] != cluster_name:
                    continue
            
            # Extract instance type from metadata
            instance_type = detail.properties.instance_name or "Unknown"
            
            # Determine purchase type (Spot, Reserved, On-Demand)
            purchase_type = "On-Demand"  # Default
            if "Spot" in detail.properties.meter_name:
                purchase_type = "Spot"
            elif "Reserved" in detail.properties.meter_name:
                purchase_type = "Reserved"
                
            # Extract vCPU count (would need to map from instance type in real implementation)
            vcpu_count = 2  # Placeholder
            
            # Extract region
            region = detail.properties.resource_location or "global"
            
            # Extract costs
            usage_quantity = float(detail.properties.quantity or 0)
            cost = float(detail.properties.cost or 0)
            cost_per_unit = cost / usage_quantity if usage_quantity > 0 else 0
            
            yield [
                usage_date.strftime('%Y-%m-%d'),
                region,
                instance_type,
                purchase_type,
                vcpu_count,
                usage_quantity,
                cost,
                cost_per_unit,
                'Azure'  # Add cloud provider column
            ]
            
    except Exception as e:
        logging.error(f"Error fetching Azure compute costs: {str(e)}")
        # Yield an error record
        yield ["ERROR", "ERROR", f"Error: {str(e)}", "", "", "", "", "", "Azure"]

def run_azure_report(start_date, end_date):
    """Run Azure cost report workflow"""
    logging.info("Fetching Azure subscriptions...")
    
    subscriptions = azure_get_subscriptions()
    
    if not subscriptions:
        logging.error("No Azure subscriptions found or authentication failed")
        print("Please ensure you're logged in with the Azure CLI or have proper environment variables set")
        return
    
    # Select subscription
    print("\nAvailable Azure Subscriptions:")
    subscription_list = [(id, name) for id, name in subscriptions.items()]
    for idx, (sub_id, sub_name) in enumerate(subscription_list, 1):
        print(f"{idx}. {sub_name} ({sub_id})")
    
    while True:
        try:
            choice = input("\nSelect subscription number (or 'q' to quit): ")
            if choice.lower() == 'q':
                return
                
            choice_idx = int(choice) - 1
            if 0 <= choice_idx < len(subscription_list):
                subscription_id, subscription_name = subscription_list[choice_idx]
                break
            else:
                print(f"Please enter a number between 1 and {len(subscription_list)}")
        except ValueError:
            print("Please enter a valid number")
    
    # Ask if we want to filter by resource group
    use_resource_group = input("\nDo you want to filter by resource group? (y/n): ").lower() == 'y'
    
    resource_group = None
    if use_resource_group:
        # Get resource groups
        resource_groups_by_region = azure_get_resource_groups(subscription_id)
        
        if resource_groups_by_region:
            _, resource_group = select_resource("Azure Resource Groups", resource_groups_by_region)
        else:
            logging.error("No resource groups found")
            print("Proceeding with subscription-wide report")
    
    # Ask if we want to filter by AKS cluster
    use_cluster = input("\nDo you want to filter by AKS cluster? (y/n): ").lower() == 'y'
    
    cluster_name = None
    if use_cluster:
        # Get AKS clusters
        clusters_by_region = azure_get_aks_clusters(subscription_id, resource_group)
        
        if clusters_by_region:
            _, cluster_name = select_resource("AKS Clusters", clusters_by_region)
        else:
            logging.error("No AKS clusters found")
            print("Proceeding without cluster filter")
    
    # Process data
    logging.info(f"Fetching Azure compute costs for subscription: {subscription_name}")
    if resource_group:
        logging.info(f"Filtering by resource group: {resource_group}")
    if cluster_name:
        logging.info(f"Filtering by cluster: {cluster_name}")
    logging.info(f"Date range: {start_date} to {end_date}")
    
    # Get the data
    cost_data = azure_fetch_compute_costs(subscription_id, start_date, end_date, resource_group, cluster_name)
    
    # CSV headers
    headers = ['date', 'region', 'instance_type', 'purchase_type', 'number_of_cpus',
               'usage_amount', 'total_cost', 'cost_per_unit', 'cloud_provider']
    
    # Determine output filename
    output_components = ['cloud_cost_report_azure', subscription_id]
    if resource_group:
        output_components.append(resource_group)
    if cluster_name:
        output_components.append(cluster_name)
    
    output_file = f"{'_'.join(output_components)}.csv"
    
    # Write to CSV
    total_rows = write_csv_in_chunks(output_file, cost_data, headers)
    
    logging.info(f"Azure CSV report generated: {output_file}")
    logging.info(f"Total rows in the report: {total_rows}")

# =====================================================================
# General Resource Selection
# =====================================================================

def select_resource(resource_type: str, resources_by_region: Dict[str, List[str]]) -> tuple:
    """Interactive resource selection with region grouping and columnar display."""
    if not resources_by_region:
        logging.error(f"No {resource_type} found")
        exit(1)

    # Create a flat list for selection while maintaining region information
    selection_map = []

    # Get terminal width for formatting
    terminal_width = get_terminal_size()

    print(f"\nAvailable {resource_type} by region:")
    print("=" * (len(f"Available {resource_type} by region:") + 5))

    current_idx = 1  # Keep track of the current resource number

    for region, resources in resources_by_region.items():
        # Print region name in color and uppercase with extra line break
        if resource_type.startswith("AWS"):
            color = GREEN
        elif resource_type.startswith("GCP"):
            color = YELLOW
        else:  # Azure
            color = BLUE
            
        print(f"\n{color}{region.upper()}{RESET}:\n")

        # Create list of resources for this region
        region_resources = []
        for resource in resources:
            selection_map.append((region, resource))
            region_resources.append(resource)

        # Format and print resources in columns
        formatted_rows = format_columns(region_resources, terminal_width, current_idx)
        for row in formatted_rows:
            print(row)

        # Update the index for the next region
        current_idx += len(resources)

    while True:
        try:
            choice = input(f"\nSelect {resource_type.lower()} number (or 'q' to quit): ")

            if choice.lower() == 'q':
                logging.info("Exiting program")
                exit(0)

            choice_idx = int(choice) - 1
            if 0 <= choice_idx < len(selection_map):
                selected_region, selected_resource = selection_map[choice_idx]
                return selected_region, selected_resource
            else:
                print(f"Please enter a number between 1 and {len(selection_map)}")
        except ValueError:
            print("Please enter a valid number")

# =====================================================================
# Main Function
# =====================================================================

def main():
    """Main function to run the Multi-Cloud Cost Explorer"""
    parser = argparse.ArgumentParser(description='Multi-Cloud Cost Explorer')
    parser.add_argument('--provider', type=str, default=None, 
                        help=f'Cloud provider to analyze. Options: {", ".join(CloudProvider.all())}')
    parser.add_argument('--start-date', type=str, default=None, 
                        help='Start date for cost analysis (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, default=None, 
                        help='End date for cost analysis (YYYY-MM-DD)')
    args = parser.parse_args()

    # Get date range
    start_date, end_date = get_date_range()
    if args.start_date:
        try:
            # Validate date format
            start_date = datetime.strptime(args.start_date, '%Y-%m-%d').strftime('%Y-%m-%d')
        except ValueError:
            logging.error("Invalid start date format. Please use YYYY-MM-DD.")
            exit(1)
    
    if args.end_date:
        try:
            # Validate date format
            end_date = datetime.strptime(args.end_date, '%Y-%m-%d').strftime('%Y-%m-%d')
        except ValueError:
            logging.error("Invalid end date format. Please use YYYY-MM-DD.")
            exit(1)

    # Select cloud provider if not specified
    provider = args.provider
    if not provider:
        print("Select cloud provider:")
        for idx, provider_name in enumerate(CloudProvider.all(), 1):
            if provider_name == CloudProvider.AWS:
                color = GREEN
            elif provider_name == CloudProvider.GCP:
                color = YELLOW
            else:  # Azure
                color = BLUE
            print(f"{idx}. {color}{provider_name.upper()}{RESET}")
        
        while True:
            try:
                choice = input("\nSelect cloud provider number: ")
                choice_idx = int(choice) - 1
                if 0 <= choice_idx < len(CloudProvider.all()):
                    provider = CloudProvider.all()[choice_idx]
                    break
                else:
                    print(f"Please enter a number between 1 and {len(CloudProvider.all())}")
            except ValueError:
                print("Please enter a valid number")
    else:
        try:
            provider = CloudProvider.validate(provider)
        except ValueError as e:
            logging.error(str(e))
            exit(1)
    
    # Run the appropriate provider workflow
    if provider == CloudProvider.AWS:
        print(f"\n{GREEN}Running AWS Cost Explorer...{RESET}")
        run_aws_report(start_date, end_date)
    elif provider == CloudProvider.GCP:
        print(f"\n{YELLOW}Running GCP Cost Explorer...{RESET}")
        run_gcp_report(start_date, end_date)
    elif provider == CloudProvider.AZURE:
        print(f"\n{BLUE}Running Azure Cost Explorer...{RESET}")
        run_azure_report(start_date, end_date)
    else:
        logging.error(f"Invalid provider: {provider}")
        exit(1)

if __name__ == "__main__":
    main()