#!/bin/bash

# ANSI color codes
RED='\033[0;31m'
YELLOW='\033[1;33m'
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Function to log messages with colors
log_message() {
    local level=$1
    local message=$2
    case $level in
        "INFO")    echo -e "${BLUE}[INFO]${NC} $message" ;;
        "SUCCESS") echo -e "${GREEN}[SUCCESS]${NC} $message" ;;
        "WARNING") echo -e "${YELLOW}[WARNING]${NC} $message" ;;
        "ERROR")   echo -e "${RED}[ERROR]${NC} $message" ;;
        *)         echo -e "$message" ;;
    esac
}

# Function to display usage information
usage() {
    echo "Usage: $0 --schema <schema_name> --tables <table1,table2,...> --container <container-name> --path <blob-path> [--delimiter <delimiter>] [--compress] [--generate-ddl] [--overwrite] [--dry-run]"
    echo "  --schema: Schema name of the tables to export"
    echo "  --tables: Comma-separated list of tables to export"
    echo "  --container: Azure Blob Storage container name"
    echo "  --path: Path within the container to store TSV files"
    echo "  --delimiter: TSV delimiter (default: '\t')"
    echo "  --compress: Compress the TSV file using gzip before uploading"
    echo "  --generate-ddl: Generate SQL DDL for the tables"
    echo "  --overwrite: Overwrite existing blobs in Azure Storage"
    echo "  --dry-run: Perform a dry run without exporting or uploading data"
    exit 1
}

# Parse command-line arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --schema) SCHEMA="$2"; shift ;;
        --tables) TABLES="$2"; shift ;;
        --container) CONTAINER="$2"; shift ;;
        --path) BLOB_PATH="$2"; shift ;;
        --delimiter) DELIMITER="$2"; shift ;;
        --compress) COMPRESS=true ;;
        --generate-ddl) GENERATE_DDL=true ;;
        --overwrite) OVERWRITE=true ;;
        --dry-run) DRY_RUN=true ;;
        *) usage ;;
    esac
    shift
done

# Validate required arguments
if [[ -z "$SCHEMA" || -z "$TABLES" || -z "$CONTAINER" || -z "$BLOB_PATH" ]]; then
    usage
fi

# Set default delimiter if not provided
DELIMITER=${DELIMITER:-$'\t'}

# Set up logging
LOG_FILE="export_log_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$LOG_FILE") 2>&1

log_message "INFO" "Starting export process at $(date)"

# Load environment variables
if [[ -f .env ]]; then
    set -a
    source .env
    set +a
    log_message "INFO" "Environment variables loaded from .env file"
else
    log_message "ERROR" "Error: .env file not found"
    exit 1
fi

# Validate environment variables
required_vars=(
    "SOURCE_AZURE_SQL_SERVER"
    "SOURCE_AZURE_SQL_DATABASE"
    "SOURCE_AZURE_SQL_USERNAME"
    "SOURCE_AZURE_SQL_PASSWORD"
    "AZURE_STORAGE_ACCOUNT"
)
for var in "${required_vars[@]}"; do
    if [[ -z "${!var}" ]]; then
        log_message "ERROR" "Error: $var is not set in the .env file"
        exit 1
    fi
done

OVERALL_SUCCESS=true

# Function to test SQL connection
test_sql_connection() {
    log_message "INFO" "Testing connection to SQL Server..."
    local error_output
    error_output=$(sqlcmd -S "$SOURCE_AZURE_SQL_SERVER" -d "$SOURCE_AZURE_SQL_DATABASE" -U "$SOURCE_AZURE_SQL_USERNAME" -P "$SOURCE_AZURE_SQL_PASSWORD" -Q "SELECT 1" -h -1 2>&1)
    local sqlcmd_exit_code=$?
    if [[ $sqlcmd_exit_code -ne 0 ]]; then
        log_message "ERROR" "Failed to connect to SQL Server. Exit code: $sqlcmd_exit_code"
        log_message "ERROR" "Error details: $error_output"
        OVERALL_SUCCESS=false
        return 1
    fi
    log_message "SUCCESS" "Successfully connected to SQL Server."
}

# Function to execute bcp and export to TSV
export_table_to_tsv() {
    local table=$1
    local tsv_file="${table}.tsv"
    
    log_message "INFO" "Exporting $SCHEMA.$table to $tsv_file"
    
    if [[ "$DRY_RUN" != true ]]; then
        local error_output
        error_output=$(bcp "SELECT * FROM [$SCHEMA].[$table]" queryout "$tsv_file" -c -t "$DELIMITER" \
            -S "$SOURCE_AZURE_SQL_SERVER" -d "$SOURCE_AZURE_SQL_DATABASE" \
            -U "$SOURCE_AZURE_SQL_USERNAME" -P "$SOURCE_AZURE_SQL_PASSWORD" 2>&1)
        
        local bcp_exit_code=$?
        if [[ $bcp_exit_code -ne 0 ]]; then
            log_message "ERROR" "Failed to export $SCHEMA.$table. BCP exit code: $bcp_exit_code"
            log_message "ERROR" "Error details: $error_output"
            OVERALL_SUCCESS=false
            return 1
        fi

        local exported_count=$(wc -l < "$tsv_file")
        local db_count=$(sqlcmd -S "$SOURCE_AZURE_SQL_SERVER" -d "$SOURCE_AZURE_SQL_DATABASE" -U "$SOURCE_AZURE_SQL_USERNAME" -P "$SOURCE_AZURE_SQL_PASSWORD" -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM [$SCHEMA].[$table]" -h -1 | tr -d '[:space:]')
        log_message "SUCCESS" "Exported $SCHEMA.$table to $tsv_file ($exported_count rows)"

        if [[ $exported_count -ne $db_count ]]; then
            log_message "WARNING" "Count mismatch. Exported: $exported_count, In DB: $db_count"
        else
            log_message "SUCCESS" "Counts match. Exported: $exported_count, In DB: $db_count"
        fi
    else
        log_message "INFO" "[Dry run] Would export $SCHEMA.$table to $tsv_file"
    fi
}

# Function to generate DDL for a table
generate_table_ddl() {
    local table=$1
    local ddl_file="${table}_ddl.sql"
    
    log_message "INFO" "Generating DDL for $SCHEMA.$table"
    
    if [[ "$DRY_RUN" != true ]]; then
        sqlcmd -S "$SOURCE_AZURE_SQL_SERVER" -d "$SOURCE_AZURE_SQL_DATABASE" -U "$SOURCE_AZURE_SQL_USERNAME" -P "$SOURCE_AZURE_SQL_PASSWORD" -Q "
        SET NOCOUNT ON;
        DECLARE @TableName NVARCHAR(128) = '$table';
        DECLARE @SchemaName NVARCHAR(128) = '$SCHEMA';
        DECLARE @SQL NVARCHAR(MAX) = '';

        -- Generate CREATE TABLE statement
        SELECT @SQL = 'CREATE TABLE [' + @SchemaName + '].[' + @TableName + '] ('

        SELECT @SQL = @SQL + CHAR(13) + CHAR(10) + 
            '    [' + c.COLUMN_NAME + '] ' + 
            c.DATA_TYPE + 
            CASE 
                WHEN c.DATA_TYPE IN ('varchar', 'nvarchar', 'char', 'nchar') 
                    THEN '(' + CASE WHEN c.CHARACTER_MAXIMUM_LENGTH = -1 THEN 'MAX' ELSE CAST(c.CHARACTER_MAXIMUM_LENGTH AS VARCHAR(5)) END + ')'
                WHEN c.DATA_TYPE IN ('decimal', 'numeric') 
                    THEN '(' + CAST(c.NUMERIC_PRECISION AS VARCHAR(5)) + ', ' + CAST(c.NUMERIC_SCALE AS VARCHAR(5)) + ')'
                ELSE ''
            END + 
            CASE WHEN c.IS_NULLABLE = 'NO' THEN ' NOT NULL' ELSE ' NULL' END + ','
        FROM INFORMATION_SCHEMA.COLUMNS c
        WHERE c.TABLE_SCHEMA = @SchemaName AND c.TABLE_NAME = @TableName
        ORDER BY c.ORDINAL_POSITION

        -- Remove the last comma
        SET @SQL = LEFT(@SQL, LEN(@SQL) - 1)

        -- Close the CREATE TABLE statement
        SET @SQL = @SQL + CHAR(13) + CHAR(10) + ');'

        -- Add primary key constraint if exists
        SELECT @SQL = @SQL + CHAR(13) + CHAR(10) + 
            'ALTER TABLE [' + @SchemaName + '].[' + @TableName + '] ADD CONSTRAINT [PK_' + @TableName + '] PRIMARY KEY CLUSTERED ('
        SELECT @SQL = @SQL + CHAR(13) + CHAR(10) + '    [' + COLUMN_NAME + '] ASC,'
        FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
        WHERE OBJECTPROPERTY(OBJECT_ID(CONSTRAINT_SCHEMA + '.' + CONSTRAINT_NAME), 'IsPrimaryKey') = 1
            AND TABLE_NAME = @TableName AND TABLE_SCHEMA = @SchemaName
        ORDER BY ORDINAL_POSITION

        -- Remove the last comma and close the constraint
        IF CHARINDEX(',', @SQL) > 0
        BEGIN
            SET @SQL = LEFT(@SQL, LEN(@SQL) - 1)
            SET @SQL = @SQL + CHAR(13) + CHAR(10) + ');'
        END
        ELSE
        BEGIN
            -- Remove the ALTER TABLE statement if no primary key
            SET @SQL = LEFT(@SQL, CHARINDEX('ALTER TABLE', @SQL) - 1)
        END

        PRINT @SQL
        " -o "$ddl_file" > /dev/null 2>&1
        
        if [[ $? -ne 0 ]]; then
            log_message "ERROR" "Failed to generate DDL for $SCHEMA.$table"
            OVERALL_SUCCESS=false
            return 1
        fi
        log_message "SUCCESS" "Generated DDL for $SCHEMA.$table"
    else
        log_message "INFO" "[Dry run] Would generate DDL for $SCHEMA.$table to $ddl_file"
    fi
}

# Function to compress file
compress_file() {
    local file=$1
    log_message "INFO" "Compressing $file"
    
    if [[ "$DRY_RUN" != true ]]; then
        gzip -f "$file" > /dev/null 2>&1
        if [[ $? -ne 0 ]]; then
            log_message "ERROR" "Failed to compress $file"
            OVERALL_SUCCESS=false
            return 1
        fi
        log_message "SUCCESS" "Compressed $file"
    else
        log_message "INFO" "[Dry run] Would compress $file"
    fi
}

# Function to upload file to Blob Storage
upload_to_blob() {
    local file=$1
    local blob_name="$BLOB_PATH/$file"
    
    log_message "INFO" "Uploading $file to $CONTAINER/$blob_name"
    
    if [[ "$DRY_RUN" != true ]]; then
        local error_output
        if [[ "$OVERWRITE" == true ]]; then
            error_output=$(az storage blob upload --account-name "$AZURE_STORAGE_ACCOUNT" \
                --container-name "$CONTAINER" --name "$blob_name" --file "$file" \
                --auth-mode login --overwrite true 2>&1)
        else
            error_output=$(az storage blob upload --account-name "$AZURE_STORAGE_ACCOUNT" \
                --container-name "$CONTAINER" --name "$blob_name" --file "$file" \
                --auth-mode login --if-none-match "*" 2>&1)
        fi
        
        local az_exit_code=$?
        if [[ $az_exit_code -ne 0 ]]; then
            if [[ "$OVERWRITE" == false ]]; then
                log_message "ERROR" "Failed to upload $file to Blob Storage. The blob may already exist. Use --overwrite to force upload."
            else
                log_message "ERROR" "Failed to upload $file to Blob Storage"
            fi
            log_message "ERROR" "Error details: $error_output"
            log_message "ERROR" "Azure CLI exit code: $az_exit_code"
            OVERALL_SUCCESS=false
            return 1
        fi
        log_message "SUCCESS" "Uploaded $file to $CONTAINER/$blob_name"
    else
        if [[ "$OVERWRITE" == true ]]; then
            log_message "INFO" "[Dry run] Would upload $file to $CONTAINER/$blob_name (overwriting if exists)"
        else
            log_message "INFO" "[Dry run] Would upload $file to $CONTAINER/$blob_name (skipping if exists)"
        fi
    fi
}

# Add this function near the other function definitions
check_azure_storage_account() {
    log_message "INFO" "Checking Azure Storage Account..."
    if [[ "$DRY_RUN" != true ]]; then
        local storage_check_output
        storage_check_output=$(az storage account show --name "$AZURE_STORAGE_ACCOUNT" 2>&1)
        local storage_check_exit_code=$?
        if [[ $storage_check_exit_code -ne 0 ]]; then
            log_message "ERROR" "Failed to access Azure Storage Account: $AZURE_STORAGE_ACCOUNT"
            log_message "ERROR" "Error details: $storage_check_output"
            log_message "ERROR" "Azure CLI exit code: $storage_check_exit_code"
            OVERALL_SUCCESS=false
            return 1
        fi
        log_message "SUCCESS" "Azure Storage Account $AZURE_STORAGE_ACCOUNT is accessible"
    else
        log_message "INFO" "[Dry run] Would check Azure Storage Account accessibility"
    fi
}

# Main execution
log_message "INFO" "Authenticating with Azure..."
if [[ "$DRY_RUN" != true ]]; then
    az account show &>/dev/null
    if [[ $? -ne 0 ]]; then
        az login --use-device-code --no-wait > /dev/null 2>&1
        while true; do
            az account show &>/dev/null
            if [[ $? -eq 0 ]]; then
                break
            fi
            sleep 5
        done
    fi
    
    if [[ $? -eq 0 ]]; then
        log_message "SUCCESS" "Authentication successful"
    else
        log_message "ERROR" "Failed to authenticate with Azure"
        exit 1
    fi
else
    log_message "INFO" "[Dry run] Would authenticate with Azure"
fi

test_sql_connection

IFS=',' read -ra TABLE_ARRAY <<< "$TABLES"
for table in "${TABLE_ARRAY[@]}"; do
    export_table_to_tsv "$table"
    if [[ $? -eq 0 ]]; then
        file="${table}.tsv"
        if [[ "$COMPRESS" == true ]]; then
            compress_file "$file"
            if [[ $? -eq 0 ]]; then
                file="${file}.gz"
            else
                log_message "ERROR" "Error compressing $file, skipping upload"
                continue
            fi
        fi
        upload_to_blob "$file"
        if [[ $? -eq 0 && "$DRY_RUN" != true ]]; then
            rm "$file"
            log_message "INFO" "Temporary file $file removed"
        fi
        
        if [[ "$GENERATE_DDL" == true ]]; then
            generate_table_ddl "$table"
            if [[ $? -eq 0 ]]; then
                upload_to_blob "${table}_ddl.sql"
                if [[ $? -eq 0 && "$DRY_RUN" != true ]]; then
                    rm "${table}_ddl.sql"
                    log_message "INFO" "Temporary file ${table}_ddl.sql removed"
                fi
            fi
        fi
    fi
done

if [[ "$OVERALL_SUCCESS" == true ]]; then
    log_message "SUCCESS" "Export process completed successfully at $(date)"
else
    log_message "ERROR" "Export process completed with errors at $(date)"
fi

# In the main execution section, after Azure authentication and before the table processing loop, add:
check_azure_storage_account
if [[ $? -ne 0 ]]; then
    log_message "ERROR" "Failed to access Azure Storage Account. Exiting."
    exit 1
fi
