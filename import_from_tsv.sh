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
    echo "Usage: $0 --schema <schema_name> --tables <table1,table2,...> --container <container-name> --path <blob-path> [--delimiter <delimiter>] [--compressed] [--dry-run]"
    echo "  --schema: Schema name of the tables to import"
    echo "  --tables: Comma-separated list of tables to import"
    echo "  --container: Azure Blob Storage container name"
    echo "  --path: Path within the container where TSV files are stored"
    echo "  --delimiter: TSV delimiter (default: '\t')"
    echo "  --compressed: Indicate that the files are compressed (gzip)"
    echo "  --dry-run: Perform a dry run without importing data"
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
        --compressed) COMPRESSED=true ;;
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
LOG_FILE="import_log_$(date +%Y%m%d_%H%M%S).log"
exec > >(tee -a "$LOG_FILE") 2>&1

log_message "INFO" "Starting import process at $(date)"

# Load environment variables
if [[ -f .env ]]; then
    source .env
else
    echo "Error: .env file not found"
    exit 1
fi

# Validate environment variables
required_vars=(
    "TARGET_AZURE_SQL_SERVER"
    "TARGET_AZURE_SQL_DATABASE"
    "TARGET_AZURE_SQL_USERNAME"
    "TARGET_AZURE_SQL_PASSWORD"
    "AZURE_STORAGE_ACCOUNT"
)
for var in "${required_vars[@]}"; do
    if [[ -z "${!var}" ]]; then
        echo "Error: $var is not set in the .env file"
        exit 1
    fi
done

# Add this near the beginning of the script, after the initial variable declarations
OVERALL_SUCCESS=true

# Function to download file from Blob Storage
download_from_blob() {
    local file=$1
    local blob_name="$BLOB_PATH/$file"
    
    log_message "INFO" "Downloading $file from $CONTAINER/$blob_name"
    
    if [[ "$DRY_RUN" != true ]]; then
        local error_output
        error_output=$(az storage blob download --account-name "$AZURE_STORAGE_ACCOUNT" \
            --container-name "$CONTAINER" --name "$blob_name" --file "$file" \
            --auth-mode login 2>&1)
        
        if [[ $? -ne 0 ]]; then
            log_message "ERROR" "Failed to download $file from Blob Storage"
            log_message "ERROR" "Error details: $error_output"
            return 1
        fi
        log_message "SUCCESS" "Download completed successfully"
    else
        log_message "INFO" "[Dry run] Would download $file from $CONTAINER/$blob_name"
    fi
}

# Function to decompress file
decompress_file() {
    local compressed_file=$1
    local uncompressed_file="${compressed_file%.gz}"
    
    log_message "INFO" "Decompressing $compressed_file"
    
    if [[ "$DRY_RUN" != true ]]; then
        if [[ -f "$uncompressed_file" ]]; then
            log_message "WARNING" "Uncompressed file $uncompressed_file already exists."
            log_message "INFO" "Removing existing compressed file $compressed_file"
            rm "$compressed_file"
            return 0
        fi
        
        gzip -d "$compressed_file" > /dev/null 2>&1
        if [[ $? -ne 0 ]]; then
            log_message "ERROR" "Error decompressing $compressed_file"
            return 1
        fi
        log_message "SUCCESS" "Decompression completed successfully"
    else
        log_message "INFO" "[Dry run] Would decompress $compressed_file"
    fi
}

# Function to import TSV to Azure SQL
import_tsv_to_sql() {
    local table=$1
    local tsv_file="${table}.tsv"
    
    log_message "INFO" "Importing $tsv_file to $SCHEMA.$table"
    
    if [[ "$DRY_RUN" != true ]]; then
        # Count existing rows in the table
        local existing_count=$(sqlcmd -S "$TARGET_AZURE_SQL_SERVER" -d "$TARGET_AZURE_SQL_DATABASE" \
            -U "$TARGET_AZURE_SQL_USERNAME" -P "$TARGET_AZURE_SQL_PASSWORD" \
            -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM [$SCHEMA].[$table]" -h -1)
        log_message "INFO" "Existing rows in table: $existing_count"

        # Import data with error file
        local error_file="${table}_errors.txt"
        local bcp_output_file="${table}_bcp_output.txt"
        local bcp_output
        bcp_output=$(bcp "[$SCHEMA].[$table]" in "$tsv_file" -c -t "$DELIMITER" \
            -S "$TARGET_AZURE_SQL_SERVER" -d "$TARGET_AZURE_SQL_DATABASE" \
            -U "$TARGET_AZURE_SQL_USERNAME" -P "$TARGET_AZURE_SQL_PASSWORD" \
            -e "$error_file" -m 1 2>&1)

        local bcp_exit_code=$?
        if [[ $bcp_exit_code -ne 0 ]]; then
            log_message "ERROR" "Failed to import $tsv_file to $SCHEMA.$table (Exit code: $bcp_exit_code)"
            log_message "ERROR" "BCP Output: $bcp_output"
            if [[ -s "$error_file" ]]; then
                log_message "ERROR" "Error details from error file:"
                cat "$error_file"
            fi
            OVERALL_SUCCESS=false
            return 1
        fi

        # Count rows after import
        local imported_count=$(sqlcmd -S "$TARGET_AZURE_SQL_SERVER" -d "$TARGET_AZURE_SQL_DATABASE" \
            -U "$TARGET_AZURE_SQL_USERNAME" -P "$TARGET_AZURE_SQL_PASSWORD" \
            -Q "SET NOCOUNT ON; SELECT COUNT(*) FROM [$SCHEMA].[$table]" -h -1)
        log_message "SUCCESS" "Rows in table after import: $imported_count"
        log_message "SUCCESS" "Imported $(($imported_count - $existing_count)) new rows"

        # Clean up error and output files
        rm -f "$error_file" "$bcp_output_file"
    else
        log_message "INFO" "[Dry run] Would import $tsv_file to $SCHEMA.$table"
    fi
}

# Simplified check_data_issues function
check_data_issues() {
    local table=$1
    local tsv_file="${table}.tsv"

    log_message "INFO" "Checking for potential data issues in $tsv_file"

    if [[ "$DRY_RUN" != true ]]; then
        # Check for empty lines
        local empty_lines=$(grep -c '^$' "$tsv_file")
        if [[ $empty_lines -gt 0 ]]; then
            log_message "WARNING" "Found $empty_lines empty lines in the TSV file"
        fi

        # Display first few columns of the table structure
        log_message "INFO" "Table structure (first 5 columns):"
        sqlcmd -S "$TARGET_AZURE_SQL_SERVER" -d "$TARGET_AZURE_SQL_DATABASE" \
            -U "$TARGET_AZURE_SQL_USERNAME" -P "$TARGET_AZURE_SQL_PASSWORD" \
            -Q "
        SET NOCOUNT ON;
        SELECT TOP 5 COLUMN_NAME, DATA_TYPE, CHARACTER_MAXIMUM_LENGTH
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '$SCHEMA' AND TABLE_NAME = '$table'
        ORDER BY ORDINAL_POSITION;
        " -h -1
    else
        log_message "INFO" "[Dry run] Would check for data issues in $tsv_file"
    fi
}

# Main execution
log_message "INFO" "Authenticating with Azure..."
if [[ "$DRY_RUN" != true ]]; then
    # Check if already logged in
    az account show &>/dev/null
    if [[ $? -ne 0 ]]; then
        # If not logged in, attempt to login non-interactively
        az login --use-device-code --no-wait > /dev/null 2>&1
        
        # Wait for authentication to complete
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
        log_message "ERROR" "Error: Failed to authenticate with Azure"
        exit 1
    fi
else
    log_message "INFO" "[Dry run] Would authenticate with Azure"
fi

IFS=',' read -ra TABLE_ARRAY <<< "$TABLES"
for table in "${TABLE_ARRAY[@]}"; do
    file="${table}.tsv"
    if [[ "$COMPRESSED" == true ]]; then
        file="${file}.gz"
    fi
    
    download_from_blob "$file"
    if [[ $? -eq 0 ]]; then
        if [[ "$COMPRESSED" == true ]]; then
            decompress_file "$file"
            if [[ $? -eq 0 ]]; then
                file="${file%.gz}"
            else
                continue
            fi
        fi
        log_message "INFO" "File size: $(wc -c < "$file") bytes"
        log_message "INFO" "Number of lines: $(wc -l < "$file")"
        # check_data_issues "$table"
        import_tsv_to_sql "$table"
        if [[ $? -eq 0 && "$DRY_RUN" != true ]]; then
            rm "$file"
            log_message "INFO" "Temporary file $file removed"
        fi
    fi
done

if [[ "$OVERALL_SUCCESS" == true ]]; then
    log_message "SUCCESS" "Import process completed successfully at $(date)"
else
    log_message "ERROR" "Import process completed with errors at $(date)"
fi
