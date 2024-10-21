# Azure SQL Data Export and Import Tools

This repository contains tools for exporting data from Azure SQL to TSV (Tab-Separated Values) files and importing TSV files back into Azure SQL.

## Export Tool

This tool exports data from Azure SQL tables to TSV (Tab-Separated Values) files and uploads them to Azure Blob Storage.

## Prerequisites

- Azure CLI installed and configured (version 2.63.0 or later)
- `sqlcmd` utility installed (part of SQL Server Command Line Tools)
- `bcp` utility installed (part of SQL Server Command Line Tools)
- Access to an Azure SQL Database
- Access to an Azure Storage Account

## Setup

1. Clone this repository or copy the `export_to_tsv.sh` script to your local machine.

2. Make the script executable:
   ``` bash
   chmod +x export_to_tsv.sh
   ```

3. Create a `.env` file in the same directory as the scripts with the following content:
   ``` plaintext
   AZURE_SQL_SERVER=your_server.database.windows.net
   AZURE_SQL_DATABASE=your_database
   AZURE_SQL_USERNAME=your_username
   AZURE_SQL_PASSWORD=your_password
   AZURE_STORAGE_ACCOUNT=your_storage_account
   ```
   Replace the placeholders with your actual Azure SQL and Storage account details.

## Usage

Run the script with the following command:

``` bash
./export_to_tsv.sh --schema <schema_name> --tables <table1,table2,...> --container <container-name> --path <blob-path> [--delimiter <delimiter>] [--compress] [--generate-ddl] [--overwrite] [--dry-run] 
```

### Parameters:

- `--schema`: Schema name of the tables to export
- `--tables`: Comma-separated list of tables to export
- `--container`: Azure Blob Storage container name
- `--path`: Path within the container to store CSV files
- `--delimiter`: (Optional) CSV delimiter (default: ',')
- `--compress`: (Optional) Compress the TSV file using gzip before uploading
- `--generate-ddl`: Generate SQL DDL for the tables
- `--overwrite`: Overwrite existing blobs in Azure Storage
- `--dry-run`: (Optional) Perform a dry run without exporting or uploading data

### Example:

``` bash
./export_to_tsv.sh --schema "archive" --tables "logs-2023-08-25" --container "sqlbackup" --path "archive/logs" --compress --generate-ddl --overwrite
```

This command will export the specified tables and upload the resulting files to Azure Blob Storage.

## Logging

The script creates a log file for each run in the same directory, named `export_log_YYYYMMDD_HHMMSS.log`. This log contains all the output from the script, including any errors encountered during the export process.

## Import Tool

The `import_from_tsv.sh` script imports TSV files from Azure Blob Storage into Azure SQL tables.

### Usage

Run the script with the following command:

``` bash
./import_from_tsv.sh --schema <schema_name> --tables <table1,table2,...> --container <container-name> --path <blob-path> [--delimiter <delimiter>] [--compressed] [--dry-run]
```

### Parameters:

- `--schema`: Schema name of the tables to import
- `--tables`: Comma-separated list of tables to import
- `--container`: Azure Blob Storage container name
- `--path`: Path within the container where TSV files are stored
- `--delimiter`: TSV delimiter (default: '\t')
- `--compressed`: Indicate that the files are compressed (gzip)
- `--dry-run`: Perform a dry run without importing data

### Example:

``` bash
./import_from_tsv.sh --schema archive --tables "logs-2023-08-25" --container "sqlbackup" --path "archive/logs" --compressed
```

This command will import the specified tables into the "archive" schema, assuming the files are compressed.

### Logging

The script creates a log file for each run in the same directory, named `import_log_YYYYMMDD_HHMMSS.log`. This log contains all the output from the script, including any errors encountered during the import process.

## Troubleshooting

1. If you encounter authentication issues, ensure that you're logged in to Azure CLI and have the necessary permissions.
2. For SQL connection issues, verify the credentials in the `.env` file and ensure your IP is allowed in the Azure SQL firewall rules.
3. For blob storage upload/download issues, check that your Azure account has the necessary permissions to write to the specified container.
4. If you encounter syntax errors related to table names, ensure that the schema and table names are correct and don't contain unsupported characters.

## Security Note

The `.env` file contains sensitive information. Ensure it's not committed to version control and is properly secured on your system.
