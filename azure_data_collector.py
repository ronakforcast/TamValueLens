from azure.identity import DefaultAzureCredential
from azure.mgmt.costmanagement import CostManagementClient
from azure.mgmt.containerservice import ContainerServiceClient
import datetime

# Set Azure Subscription ID
# We can consider fetching this as an input for the script
SUBSCRIPTION_ID = "<<SUBSCRIPTION_ID>>"

# Authenticate with Azure
credential = DefaultAzureCredential()
cost_client = CostManagementClient(credential)
clusters_client = ContainerServiceClient(credential, SUBSCRIPTION_ID)

# Fetch Cost Details for AKS Cluster
def get_aks_cost(resource_group):
    scope = f"/subscriptions/{SUBSCRIPTION_ID}/resourceGroups/{resource_group}"
    from_date = datetime.datetime.now(datetime.timezone.utc) - datetime.timedelta(days=364) # Seems like we can only go back 1 year, otherwise I was getting the error: Error fetching AKS cost: (BadRequest) Invalid query definition: The time period for pulling the data cannot exceed 1 year(s)
    to_date = datetime.datetime.now(datetime.timezone.utc)
    query = {
        "type": "ActualCost",
        "timeframe": "Custom",
        "time_period": {
            "from_property": from_date.isoformat(),
            "to": to_date.isoformat()
        },
        "dataset": {
            "granularity": "Daily",
            "aggregation": {
                "totalCost": {
                    "name": "Cost", # We have different aggregation values here, so might need to figure out which one to use. Available are: 'UsageQuantity','PreTaxCost','Cost','CostUSD','PreTaxCostUSD'
                    "function": "Sum"
                }
            }
        }
    }
    
    try:
        cost_data = cost_client.query.usage(scope, query)
        return cost_data
    except Exception as e:
        print(f"Error fetching AKS cost: {e}")
        return None

# List All AKS Clusters in the Subscription
def list_aks_clusters():
    clusters = clusters_client.managed_clusters.list()
    cluster_details = []
    for cluster in clusters:
        cluster_details.append({
            "cluster_name": cluster.name,
            "cluster_node_resource_group": cluster.node_resource_group,
            "cluster_resource_group": cluster.id.split("/")[4]
        })
    return cluster_details

# Save Data to CSV
def save_to_csv(cluster_name, cost):
    filename = f"aks_cluster_cost_{cluster_name}.csv"
    with open(filename, mode='w', newline='') as file:
        columns = ""
        for column in cost.columns:
            columns = columns + column.name + ","
        file.write(columns + '\n')
        for row in cost.rows:
            row = ",".join(str(s) for s in row)
            file.write(row + '\n')
    print(f"Data saved to {filename}")

# Execute the script
if __name__ == "__main__":
    print(f"Fetching all clusters on subscription: {SUBSCRIPTION_ID}")
    aks_clusters = list_aks_clusters()
    for cluster in aks_clusters:
        #If needed we can extend this to also collect cost for the cluster resource group, which contains the managed cluster itself, but not the nodes itself
        print(f"Fetching cost data for cluster: {cluster['cluster_name']} and cluster node resource group: {cluster['cluster_node_resource_group']}")
        aks_cost = get_aks_cost(cluster["cluster_node_resource_group"])
        save_to_csv(cluster["cluster_name"], aks_cost)
