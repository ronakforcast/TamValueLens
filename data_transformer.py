#!/usr/bin/env python3
"""
Multi-Cloud Usage Data Processor

This module processes cost report files from multiple cloud providers (AWS, GCP, Azure)
and aggregates them into a master CSV file. It detects the cloud provider from the filename,
applies appropriate processing logic, and combines results.
"""

import logging
import re
from pathlib import Path
from typing import Dict, List, Optional, Callable
from abc import ABC, abstractmethod

import pandas as pd


# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


class CloudCostProcessorBase(ABC):
    """Base abstract class for cloud-specific cost processors."""
    
    def __init__(self, filename: str):
        """
        Initialize the cloud-specific processor.
        
        Args:
            filename: The raw filename being processed
        """
        self.filename = filename
    
    @abstractmethod
    def extract_cluster_name(self) -> str:
        """Extract the cluster name from the filename."""
        pass
    
    @abstractmethod
    def process_file(self, file_path: Path) -> pd.DataFrame:
        """
        Process a single cloud provider file.
        
        Args:
            file_path: Path to the file to process
            
        Returns:
            DataFrame with the processed data in standardized format
        """
        pass


class AWSCostProcessor(CloudCostProcessorBase):
    """Processor for AWS EC2 cost reports."""
    
    def __init__(self, filename: str):
        super().__init__(filename)
        self.filename_pattern = r"aws_ec2_cost_report_(.*?)_.*?\.csv"
    
    def extract_cluster_name(self) -> str:
        """Extract cluster name from AWS filename using regex pattern."""
        match = re.match(self.filename_pattern, self.filename)
        cluster_name = match.group(1) if match else "unknown-aws"
        
        if cluster_name == "unknown-aws":
            logger.warning(f"Could not extract cluster name from AWS filename: {self.filename}")
        
        return cluster_name
    
    def process_file(self, file_path: Path) -> pd.DataFrame:
        """Process an AWS cost report file."""
        logger.info(f"Processing AWS file: {file_path}")
        
        try:
            df = pd.read_csv(file_path)
            
            # Check for required columns
            required_columns = ["date", "purchase_type", "number_of_cpus", "total_cost"]
            missing_columns = [col for col in required_columns if col not in df.columns]
            
            if missing_columns:
                raise ValueError(f"Missing required columns: {', '.join(missing_columns)}")
            
            # Create spot instance mask for better readability
            spot_mask = df["purchase_type"].str.contains("Spot", na=False)
            ondemand_mask = ~spot_mask
            
            cluster_name = self.extract_cluster_name()
            
            # Aggregate data by date
            result = df.groupby("date").apply(lambda group: pd.Series({
                "cloud_provider": "aws",
                "cluster_name": cluster_name,
                "number_of_ondemand_cpus": group.loc[ondemand_mask, "number_of_cpus"].sum(),
                "number_of_spot_cpus": group.loc[spot_mask, "number_of_cpus"].sum(),
                "total_cpu": group["number_of_cpus"].sum(),
                "total_cost_ondemand": group.loc[ondemand_mask, "total_cost"].sum(),
                "total_cost_spot": group.loc[spot_mask, "total_cost"].sum(),
                "total_cost": group["total_cost"].sum(),
                # These fields are placeholders for future calculations
                "requested_cpu_ondemand": None,
                "requested_cpu_spot": None,
                "avg_cost_per_cpu": None,
                "projected_cost": None,
                "cost_per_requested_cpu": None,
                "savings": None
            })).reset_index()
            
            # Add calculated fields
            result["avg_cost_per_cpu"] = (
                result["total_cost"] / 
                result["total_cpu"].replace(0, float('nan'))
            )
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing AWS file {file_path}: {str(e)}")
            raise


class GCPCostProcessor(CloudCostProcessorBase):
    """Processor for Google Cloud Platform cost reports."""
    
    def __init__(self, filename: str):
        super().__init__(filename)
        self.filename_pattern = r"gcp_compute_cost_report_(.*?)_.*?\.csv"
    
    def extract_cluster_name(self) -> str:
        """Extract cluster name from GCP filename using regex pattern."""
        match = re.match(self.filename_pattern, self.filename)
        cluster_name = match.group(1) if match else "unknown-gcp"
        
        if cluster_name == "unknown-gcp":
            logger.warning(f"Could not extract cluster name from GCP filename: {self.filename}")
        
        return cluster_name
    
    def process_file(self, file_path: Path) -> pd.DataFrame:
        """Process a GCP cost report file."""
        logger.info(f"Processing GCP file: {file_path}")
        
        try:
            df = pd.read_csv(file_path)
            
            # Placeholder for GCP-specific processing logic
            # This would need to be implemented based on GCP's CSV format
            # For now, creating a simplified example structure
            
            # Example: Assuming GCP CSV has "date", "machine_type", "cores", "cost" columns
            cluster_name = self.extract_cluster_name()
            
            # GCP might use different terms for spot instances (e.g., preemptible VMs)
            # Adjust according to actual GCP terminology and CSV structure
            preemptible_mask = df.get("machine_type", "").str.contains("preemptible", case=False, na=False)
            ondemand_mask = ~preemptible_mask
            
            # Group by date for consistency with other cloud providers
            result = df.groupby("date").apply(lambda group: pd.Series({
                "cloud_provider": "gcp",
                "cluster_name": cluster_name,
                "number_of_ondemand_cpus": group.loc[ondemand_mask, "cores"].sum() if "cores" in df.columns else 0,
                "number_of_spot_cpus": group.loc[preemptible_mask, "cores"].sum() if "cores" in df.columns else 0,
                "total_cpu": group["cores"].sum() if "cores" in df.columns else 0,
                "total_cost_ondemand": group.loc[ondemand_mask, "cost"].sum() if "cost" in df.columns else 0,
                "total_cost_spot": group.loc[preemptible_mask, "cost"].sum() if "cost" in df.columns else 0,
                "total_cost": group["cost"].sum() if "cost" in df.columns else 0,
                # Placeholders for additional fields
                "requested_cpu_ondemand": None,
                "requested_cpu_spot": None,
                "avg_cost_per_cpu": None,
                "projected_cost": None,
                "cost_per_requested_cpu": None,
                "savings": None
            })).reset_index()
            
            # Calculate average cost per CPU
            if "cores" in df.columns and "cost" in df.columns:
                result["avg_cost_per_cpu"] = (
                    result["total_cost"] / 
                    result["total_cpu"].replace(0, float('nan'))
                )
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing GCP file {file_path}: {str(e)}")
            raise


class AzureCostProcessor(CloudCostProcessorBase):
    """Processor for Azure cost reports."""
    
    def __init__(self, filename: str):
        super().__init__(filename)
        self.filename_pattern = r"azure_vm_cost_report_(.*?)_.*?\.csv"
    
    def extract_cluster_name(self) -> str:
        """Extract cluster name from Azure filename using regex pattern."""
        match = re.match(self.filename_pattern, self.filename)
        cluster_name = match.group(1) if match else "unknown-azure"
        
        if cluster_name == "unknown-azure":
            logger.warning(f"Could not extract cluster name from Azure filename: {self.filename}")
        
        return cluster_name
    
    def process_file(self, file_path: Path) -> pd.DataFrame:
        """Process an Azure cost report file."""
        logger.info(f"Processing Azure file: {file_path}")
        
        try:
            df = pd.read_csv(file_path)
            
            # Placeholder for Azure-specific processing logic
            # This would need to be implemented based on Azure's CSV format
            # For now, creating a simplified example structure
            
            # Example: Assuming Azure CSV has "date", "vm_type", "vcpu_count", "cost" columns
            cluster_name = self.extract_cluster_name()
            
            # Azure might use different terms for spot instances (e.g., Spot VMs)
            # Adjust according to actual Azure terminology and CSV structure
            spot_mask = df.get("vm_type", "").str.contains("spot", case=False, na=False)
            ondemand_mask = ~spot_mask
            
            # Group by date for consistency with other cloud providers
            result = df.groupby("date").apply(lambda group: pd.Series({
                "cloud_provider": "azure",
                "cluster_name": cluster_name,
                "number_of_ondemand_cpus": group.loc[ondemand_mask, "vcpu_count"].sum() if "vcpu_count" in df.columns else 0,
                "number_of_spot_cpus": group.loc[spot_mask, "vcpu_count"].sum() if "vcpu_count" in df.columns else 0,
                "total_cpu": group["vcpu_count"].sum() if "vcpu_count" in df.columns else 0,
                "total_cost_ondemand": group.loc[ondemand_mask, "cost"].sum() if "cost" in df.columns else 0,
                "total_cost_spot": group.loc[spot_mask, "cost"].sum() if "cost" in df.columns else 0,
                "total_cost": group["cost"].sum() if "cost" in df.columns else 0,
                # Placeholders for additional fields
                "requested_cpu_ondemand": None,
                "requested_cpu_spot": None,
                "avg_cost_per_cpu": None,
                "projected_cost": None,
                "cost_per_requested_cpu": None,
                "savings": None
            })).reset_index()
            
            # Calculate average cost per CPU
            if "vcpu_count" in df.columns and "cost" in df.columns:
                result["avg_cost_per_cpu"] = (
                    result["total_cost"] / 
                    result["total_cpu"].replace(0, float('nan'))
                )
            
            return result
            
        except Exception as e:
            logger.error(f"Error processing Azure file {file_path}: {str(e)}")
            raise


class MultiCloudCostProcessor:
    """Process cost data from multiple cloud providers and aggregate into a master dataset."""
    
    def __init__(self, raw_csv_folder: str, master_csv_path: str, api_key: Optional[str] = None):
        """
        Initialize the multi-cloud processor with file paths and API credentials.
        
        Args:
            raw_csv_folder: Directory containing raw CSV cost reports
            master_csv_path: Path where the aggregated master CSV will be saved
            api_key: Optional API key for additional data enrichment
        """
        self.raw_folder = Path(raw_csv_folder)
        self.master_path = Path(master_csv_path)
        self.api_key = api_key
        
        # Map file patterns to appropriate processors
        self.processor_patterns = {
            r"aws.*\.csv": AWSCostProcessor,
            r"gcp.*\.csv": GCPCostProcessor,
            r"azure.*\.csv": AzureCostProcessor,
        }
    
    def validate_paths(self) -> None:
        """Validate input folder exists and create output folder if needed."""
        if not self.raw_folder.exists():
            raise FileNotFoundError(f"Raw CSV folder not found: {self.raw_folder}")
        
        # Create parent directory for master CSV if needed
        self.master_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"Paths validated. Output will be saved to {self.master_path}")

    def get_processor_for_file(self, filename: str) -> Optional[CloudCostProcessorBase]:
        """
        Determine the appropriate processor for a given filename.
        
        Args:
            filename: Name of the file to process
            
        Returns:
            Instance of appropriate cloud processor or None if no match
        """
        for pattern, processor_class in self.processor_patterns.items():
            if re.match(pattern, filename, re.IGNORECASE):
                return processor_class(filename)
        
        logger.warning(f"No processor found for file: {filename}")
        return None

    def process_all_files(self) -> pd.DataFrame:
        """
        Process all CSV files in the raw folder using appropriate cloud processors.
        
        Returns:
            Combined DataFrame with data from all files and cloud providers
        """
        master_data = []
        processed_files = 0
        skipped_files = 0
        total_files = len(list(self.raw_folder.glob("*.csv")))
        
        logger.info(f"Found {total_files} CSV files to process")
        
        # Track metrics by cloud provider
        provider_counts = {"aws": 0, "gcp": 0, "azure": 0, "unknown": 0}
        
        # Process each CSV file
        for file in self.raw_folder.glob("*.csv"):
            try:
                processor = self.get_processor_for_file(file.name)
                
                if processor is None:
                    logger.warning(f"Skipping file with unknown format: {file.name}")
                    skipped_files += 1
                    provider_counts["unknown"] += 1
                    continue
                
                # Process the file with the appropriate processor
                processed_data = processor.process_file(file)
                
                # Track which cloud provider we processed
                if "cloud_provider" in processed_data.columns:
                    provider = processed_data["cloud_provider"].iloc[0]
                    provider_counts[provider] += 1
                
                # Add to master data
                master_data.append(processed_data)
                processed_files += 1
                
            except Exception as e:
                logger.error(f"Skipping file {file} due to error: {str(e)}")
                skipped_files += 1
                continue

        if not master_data:
            raise ValueError("No data was processed successfully")
            
        logger.info(f"Successfully processed {processed_files} of {total_files} files")
        logger.info(f"Files processed by provider: AWS={provider_counts['aws']}, "
                   f"GCP={provider_counts['gcp']}, Azure={provider_counts['azure']}, "
                   f"Unknown={provider_counts['unknown']}")
        
        # Combine all processed data
        return pd.concat(master_data, ignore_index=True)
    
    def enrich_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Enrich the combined data with additional metrics or external data.
        
        Args:
            df: Combined DataFrame from all processors
            
        Returns:
            Enriched DataFrame with additional metrics
        """
        # This is a placeholder for future data enrichment
        # Could use the API key to fetch additional data or perform calculations
        
        # Example: Add a timestamp for when this report was generated
        df["report_generated"] = pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Example: Calculate cloud-specific savings metrics
        df["potential_savings_pct"] = (
            (df["total_cost_ondemand"] - df["total_cost_spot"]) / 
            df["total_cost_ondemand"].replace(0, float('nan')) * 100
        ).round(2)
        
        return df

    def run(self) -> None:
        """Main method to execute the full processing workflow."""
        try:
            logger.info("Starting multi-cloud usage data processing")
            
            # Validate input and output paths
            self.validate_paths()
            
            # Process all files and combine results
            master_df = self.process_all_files()
            
            # Enrich the data with additional metrics
            master_df = self.enrich_data(master_df)
            
            # Save to master CSV file
            master_df.to_csv(self.master_path, index=False)
            
            # Log summary statistics
            provider_stats = master_df.groupby("cloud_provider").agg(
                clusters=("cluster_name", "nunique"),
                total_cost=("total_cost", "sum"),
                total_cpus=("total_cpu", "sum")
            )
            
            logger.info(f"Master CSV saved successfully at: {self.master_path}")
            logger.info(f"Processed data summary: {len(master_df)} rows")
            logger.info(f"Cloud provider statistics:\n{provider_stats}")
            
        except Exception as e:
            logger.error(f"Failed to process cloud usage data: {str(e)}")
            raise


def main():
    """Entry point for the script."""
    # Example usage with configuration
    RAW_CSV_FOLDER = "raw_csvs"
    MASTER_CSV_PATH = "master_cloud_usage.csv"
    API_KEY = "XYZ"  # Consider loading from environment variable
    
    processor = MultiCloudCostProcessor(RAW_CSV_FOLDER, MASTER_CSV_PATH, API_KEY)
    processor.run()


if __name__ == "__main__":
    main()