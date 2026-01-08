"""
CDC BRFSS Mental Health Data Ingestion Pipeline

This script fetches BRFSS mental health data from the CDC API,
performs validation checks, and loads it into BigQuery as a raw table.

Author: Adam Danisovszky
Date: 2026-01-08
"""

import requests
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account
import sys
from datetime import datetime
from typing import Dict, List, Tuple
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Configuration
PROJECT_ID = "enduring-broker-483700-j3"
DATASET_ID = "candidate_adamdanisovszky_mental_health"  # Replace 'yourname' with your actual name
TABLE_ID = "raw_cdc_brfss"
CDC_API_BASE = "https://data.cdc.gov/resource/dttw-5yxu.json"
SERVICE_ACCOUNT_KEY = "enduring-broker-483700-j3@appspot.gserviceaccount.com"  # Optional: set to None for default credentials

# Validation configuration
EXPECTED_COLUMNS = [
    'year', 'locationabbr', 'locationdesc', 'class', 'topic', 
    'question', 'data_value', 'sample_size'
]
MIN_ROW_COUNT = 100
MAX_NULL_PERCENTAGE = 50.0  # Maximum percentage of nulls allowed per column


class ValidationError(Exception):
    """Custom exception for validation failures"""
    pass


def fetch_brfss_data(limit: int = 10000, offset: int = 0, filters: Dict = None) -> List[Dict]:
    """
    Fetch data from CDC BRFSS API
    
    Args:
        limit: Number of records to fetch per request
        offset: Starting position for pagination
        filters: Dictionary of filter parameters
    
    Returns:
        List of records from the API
    
    Raises:
        requests.exceptions.RequestException: If API request fails
    """
    params = {
        "$limit": limit,
        "$offset": offset
    }
    
    if filters:
        params.update(filters)
    
    logger.info(f"Fetching data from CDC API (offset: {offset}, limit: {limit})...")
    
    try:
        response = requests.get(CDC_API_BASE, params=params, timeout=60)
        response.raise_for_status()
        data = response.json()
        logger.info(f"Successfully fetched {len(data)} records")
        return data
    except requests.exceptions.RequestException as e:
        logger.error(f"Error fetching data from CDC API: {e}")
        raise


def fetch_all_brfss_data(filters: Dict = None, max_records: int = None) -> List[Dict]:
    """
    Fetch all available BRFSS data with pagination
    
    Args:
        filters: Dictionary of filter parameters
        max_records: Maximum number of records to fetch (None for all)
    
    Returns:
        List of all records
    """
    all_data = []
    offset = 0
    limit = 10000
    
    logger.info("Starting data extraction from CDC BRFSS API...")
    
    while True:
        data = fetch_brfss_data(limit=limit, offset=offset, filters=filters)
        
        if not data:
            break
        
        all_data.extend(data)
        
        if max_records and len(all_data) >= max_records:
            all_data = all_data[:max_records]
            logger.info(f"Reached max_records limit: {max_records}")
            break
        
        if len(data) < limit:
            break
        
        offset += limit
    
    logger.info(f"Total records extracted: {len(all_data)}")
    return all_data


def validate_row_count(df: pd.DataFrame) -> None:
    """
    Validate that the DataFrame has a minimum number of rows
    
    Args:
        df: DataFrame to validate
    
    Raises:
        ValidationError: If row count is below minimum
    """
    row_count = len(df)
    logger.info(f"Row count validation: {row_count} rows")
    
    if row_count < MIN_ROW_COUNT:
        raise ValidationError(
            f"Row count validation failed: {row_count} rows "
            f"(minimum required: {MIN_ROW_COUNT})"
        )
    
    logger.info(f"✓ Row count validation passed: {row_count} rows")


def validate_schema(df: pd.DataFrame) -> None:
    """
    Validate that the DataFrame contains expected columns
    
    Args:
        df: DataFrame to validate
    
    Raises:
        ValidationError: If expected columns are missing
    """
    actual_columns = set(df.columns)
    expected_columns = set(EXPECTED_COLUMNS)
    
    logger.info(f"Schema validation: checking for expected columns...")
    logger.info(f"Actual columns: {sorted(actual_columns)}")
    
    # Check for missing expected columns (warnings only, not strict)
    missing_columns = expected_columns - actual_columns
    if missing_columns:
        logger.warning(
            f"⚠ Some expected columns are missing: {sorted(missing_columns)}"
        )
    
    # Ensure we have at least some columns
    if len(actual_columns) == 0:
        raise ValidationError("Schema validation failed: DataFrame has no columns")
    
    logger.info(f"✓ Schema validation passed: {len(actual_columns)} columns present")


def validate_null_checks(df: pd.DataFrame) -> None:
    """
    Perform basic null value checks on the DataFrame
    
    Args:
        df: DataFrame to validate
    
    Raises:
        ValidationError: If null percentage exceeds threshold for critical columns
    """
    logger.info("Null value validation: checking null percentages...")
    
    null_report = []
    critical_failures = []
    
    for column in df.columns:
        null_count = df[column].isna().sum()
        null_percentage = (null_count / len(df)) * 100
        
        null_report.append({
            'column': column,
            'null_count': null_count,
            'null_percentage': round(null_percentage, 2)
        })
        
        if null_percentage > MAX_NULL_PERCENTAGE:
            critical_failures.append(
                f"{column}: {null_percentage:.2f}% nulls (limit: {MAX_NULL_PERCENTAGE}%)"
            )
    
    # Log null report
    null_df = pd.DataFrame(null_report).sort_values('null_percentage', ascending=False)
    logger.info(f"\nNull Value Report:\n{null_df.to_string(index=False)}")
    
    # Check for critical failures
    if critical_failures:
        logger.warning(
            f"⚠ High null percentages detected:\n" + "\n".join(critical_failures)
        )
    else:
        logger.info(f"✓ Null validation passed: All columns within acceptable limits")


def validate_data(df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Run all validation checks on the DataFrame
    
    Args:
        df: DataFrame to validate
    
    Returns:
        Tuple of (validation_passed, list_of_warnings)
    """
    logger.info("\n" + "="*60)
    logger.info("STARTING DATA VALIDATION")
    logger.info("="*60)
    
    warnings = []
    
    try:
        # Run validation checks
        validate_row_count(df)
        validate_schema(df)
        validate_null_checks(df)
        
        logger.info("\n" + "="*60)
        logger.info("✓ ALL VALIDATION CHECKS PASSED")
        logger.info("="*60 + "\n")
        
        return True, warnings
        
    except ValidationError as e:
        logger.error(f"\n✗ VALIDATION FAILED: {e}")
        logger.info("="*60 + "\n")
        return False, [str(e)]


def load_to_bigquery(
    df: pd.DataFrame, 
    project_id: str, 
    dataset_id: str, 
    table_id: str, 
    credentials_path: str = None
) -> bigquery.Table:
    """
    Load DataFrame into BigQuery as a raw table
    
    Args:
        df: DataFrame containing the data
        project_id: GCP project ID
        dataset_id: BigQuery dataset ID
        table_id: BigQuery table ID
        credentials_path: Path to service account key file
    
    Returns:
        BigQuery Table object
    """
    logger.info("\n" + "="*60)
    logger.info("LOADING DATA TO BIGQUERY")
    logger.info("="*60)
    
    # Initialize BigQuery client
    if credentials_path:
        credentials = service_account.Credentials.from_service_account_file(
            credentials_path,
            scopes=["https://www.googleapis.com/auth/cloud-platform"]
        )
        client = bigquery.Client(credentials=credentials, project=project_id)
    else:
        client = bigquery.Client(project=project_id)
    
    # Create dataset if it doesn't exist
    dataset_ref = f"{project_id}.{dataset_id}"
    try:
        client.get_dataset(dataset_ref)
        logger.info(f"Dataset {dataset_ref} already exists")
    except Exception:
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset)
        logger.info(f"Created dataset: {dataset_ref}")
    
    # Define table reference
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    
    # Configure load job
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=True,
    )
    
    logger.info(f"Loading {len(df)} rows to {table_ref}...")
    
    # Load data
    job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    job.result()  # Wait for job to complete
    
    table = client.get_table(table_ref)
    logger.info(f"✓ Successfully loaded {table.num_rows} rows to {table_ref}")
    logger.info("="*60 + "\n")
    
    return table


def main():
    """
    Main execution function
    """
    start_time = datetime.now()
    
    logger.info("\n" + "="*60)
    logger.info("CDC BRFSS MENTAL HEALTH DATA INGESTION PIPELINE")
    logger.info(f"Started at: {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("="*60 + "\n")
    
    try:
        # Step 1: Extract data from CDC API
        logger.info("STEP 1: EXTRACTING DATA FROM CDC API")
        logger.info("-"*60)
        
        filters = {
            # Add filters as needed, e.g.:
            # '$where': "topic='Mental Health'"
        }
        
        data = fetch_all_brfss_data(filters=filters, max_records=None)
        
        if not data:
            logger.error("No data fetched from CDC API. Exiting.")
            sys.exit(1)
        
        # Convert to DataFrame
        df = pd.DataFrame(data)
        logger.info(f"Data shape: {df.shape}")
        logger.info(f"Columns: {df.columns.tolist()}\n")
        
        # Step 2: Validate data
        logger.info("STEP 2: VALIDATING DATA")
        logger.info("-"*60)
        
        validation_passed, warnings = validate_data(df)
        
        if not validation_passed:
            logger.error("Data validation failed. Exiting.")
            sys.exit(1)
        
        # Step 3: Load to BigQuery
        logger.info("STEP 3: LOADING TO BIGQUERY")
        logger.info("-"*60)
        
        table = load_to_bigquery(
            df=df,
            project_id=PROJECT_ID,
            dataset_id=DATASET_ID,
            table_id=TABLE_ID,
            credentials_path=SERVICE_ACCOUNT_KEY if SERVICE_ACCOUNT_KEY != "path/to/your-service-account-key.json" else None
        )
        
        # Summary
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("="*60)
        logger.info("✓ PIPELINE COMPLETED SUCCESSFULLY")
        logger.info("="*60)
        logger.info(f"Total rows processed: {len(df)}")
        logger.info(f"BigQuery table: {table.project}.{table.dataset_id}.{table.table_id}")
        logger.info(f"Duration: {duration:.2f} seconds")
        logger.info(f"Completed at: {end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        logger.info("="*60 + "\n")
        
    except Exception as e:
        logger.error(f"\n✗ PIPELINE FAILED: {e}", exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
