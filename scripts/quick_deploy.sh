#!/bin/bash
# =============================================================================
# Flux Data Forge - Quick Deploy Script
# =============================================================================
# This script automates the deployment of Flux Data Forge to Snowflake SPCS.
#
# Usage:
#   ./scripts/quick_deploy.sh
#
# Prerequisites:
#   - Docker installed and running
#   - Snowflake CLI (snow) installed, OR manual registry login
#   - Environment variables set (see below)
# =============================================================================

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# -----------------------------------------------------------------------------
# CONFIGURATION - Set these or export as environment variables
# -----------------------------------------------------------------------------
SNOWFLAKE_ACCOUNT="${SNOWFLAKE_ACCOUNT:-}"
SNOWFLAKE_USER="${SNOWFLAKE_USER:-}"
SNOWFLAKE_DATABASE="${SNOWFLAKE_DATABASE:-}"
SNOWFLAKE_SCHEMA="${SNOWFLAKE_SCHEMA:-}"
SNOWFLAKE_WAREHOUSE="${SNOWFLAKE_WAREHOUSE:-}"
SNOWFLAKE_ROLE="${SNOWFLAKE_ROLE:-SYSADMIN}"
COMPUTE_POOL="${COMPUTE_POOL:-}"
IMAGE_REPO="${IMAGE_REPO:-FLUX_DATA_FORGE_REPO}"
SERVICE_NAME="${SERVICE_NAME:-FLUX_DATA_FORGE_SERVICE}"
IMAGE_TAG="${IMAGE_TAG:-latest}"

# -----------------------------------------------------------------------------
# HELPER FUNCTIONS
# -----------------------------------------------------------------------------

print_header() {
    echo ""
    echo -e "${BLUE}=============================================================================${NC}"
    echo -e "${BLUE}$1${NC}"
    echo -e "${BLUE}=============================================================================${NC}"
}

print_step() {
    echo -e "${GREEN}▶ $1${NC}"
}

print_warning() {
    echo -e "${YELLOW}⚠ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

check_required_var() {
    local var_name=$1
    local var_value=${!var_name}
    
    if [ -z "$var_value" ]; then
        print_error "Missing required variable: $var_name"
        return 1
    fi
    return 0
}

# -----------------------------------------------------------------------------
# PRE-FLIGHT CHECKS
# -----------------------------------------------------------------------------

print_header "Flux Data Forge - Quick Deploy"

echo ""
echo "This script will:"
echo "  1. Validate configuration"
echo "  2. Build Docker image"
echo "  3. Push to Snowflake Image Registry"
echo "  4. Generate deployment SQL"
echo ""

# Check for Docker
if ! command -v docker &> /dev/null; then
    print_error "Docker is not installed. Please install Docker first."
    exit 1
fi
print_success "Docker found"

# Check Docker is running
if ! docker info &> /dev/null; then
    print_error "Docker is not running. Please start Docker first."
    exit 1
fi
print_success "Docker is running"

# -----------------------------------------------------------------------------
# INTERACTIVE CONFIGURATION
# -----------------------------------------------------------------------------

print_header "Configuration"

# Prompt for missing variables
if [ -z "$SNOWFLAKE_ACCOUNT" ]; then
    echo -n "Snowflake Account (org-account format): "
    read SNOWFLAKE_ACCOUNT
fi

if [ -z "$SNOWFLAKE_USER" ]; then
    echo -n "Snowflake Username: "
    read SNOWFLAKE_USER
fi

if [ -z "$SNOWFLAKE_DATABASE" ]; then
    echo -n "Database name: "
    read SNOWFLAKE_DATABASE
fi

if [ -z "$SNOWFLAKE_SCHEMA" ]; then
    echo -n "Schema name: "
    read SNOWFLAKE_SCHEMA
fi

if [ -z "$SNOWFLAKE_WAREHOUSE" ]; then
    echo -n "Warehouse name: "
    read SNOWFLAKE_WAREHOUSE
fi

if [ -z "$COMPUTE_POOL" ]; then
    echo -n "Compute Pool name: "
    read COMPUTE_POOL
fi

# Validate all required variables
print_step "Validating configuration..."
MISSING=0
check_required_var "SNOWFLAKE_ACCOUNT" || MISSING=1
check_required_var "SNOWFLAKE_USER" || MISSING=1
check_required_var "SNOWFLAKE_DATABASE" || MISSING=1
check_required_var "SNOWFLAKE_SCHEMA" || MISSING=1
check_required_var "SNOWFLAKE_WAREHOUSE" || MISSING=1
check_required_var "COMPUTE_POOL" || MISSING=1

if [ $MISSING -eq 1 ]; then
    print_error "Missing required configuration. Exiting."
    exit 1
fi

# Derive registry URL
REGISTRY_URL="${SNOWFLAKE_ACCOUNT}.registry.snowflakecomputing.com"
FULL_IMAGE="${REGISTRY_URL}/${SNOWFLAKE_DATABASE}/${SNOWFLAKE_SCHEMA}/${IMAGE_REPO}/flux_data_forge:${IMAGE_TAG}"

echo ""
print_success "Configuration validated"
echo ""
echo "  Account:      $SNOWFLAKE_ACCOUNT"
echo "  Database:     $SNOWFLAKE_DATABASE"
echo "  Schema:       $SNOWFLAKE_SCHEMA"
echo "  Warehouse:    $SNOWFLAKE_WAREHOUSE"
echo "  Compute Pool: $COMPUTE_POOL"
echo "  Image:        $FULL_IMAGE"
echo ""

# Confirm
echo -n "Proceed with deployment? (y/N): "
read CONFIRM
if [[ ! "$CONFIRM" =~ ^[Yy]$ ]]; then
    echo "Deployment cancelled."
    exit 0
fi

# -----------------------------------------------------------------------------
# STEP 1: LOGIN TO REGISTRY
# -----------------------------------------------------------------------------

print_header "Step 1: Login to Snowflake Registry"

print_step "Logging in to $REGISTRY_URL..."
echo "Enter your Snowflake password when prompted."

if ! docker login "$REGISTRY_URL" -u "$SNOWFLAKE_USER"; then
    print_error "Failed to login to Snowflake registry"
    print_warning "Make sure your password is correct and the image repository exists"
    exit 1
fi
print_success "Logged in to Snowflake registry"

# -----------------------------------------------------------------------------
# STEP 2: BUILD DOCKER IMAGE
# -----------------------------------------------------------------------------

print_header "Step 2: Build Docker Image"

# Find script directory and project root
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

print_step "Building Docker image from $PROJECT_ROOT/spcs_app..."

cd "$PROJECT_ROOT/spcs_app"

if ! docker build -t "flux_data_forge:${IMAGE_TAG}" .; then
    print_error "Docker build failed"
    exit 1
fi
print_success "Docker image built successfully"

# -----------------------------------------------------------------------------
# STEP 3: TAG AND PUSH
# -----------------------------------------------------------------------------

print_header "Step 3: Push to Snowflake Registry"

print_step "Tagging image..."
docker tag "flux_data_forge:${IMAGE_TAG}" "$FULL_IMAGE"

print_step "Pushing image to Snowflake..."
if ! docker push "$FULL_IMAGE"; then
    print_error "Failed to push image"
    print_warning "Make sure the image repository exists in Snowflake:"
    echo "  CREATE IMAGE REPOSITORY IF NOT EXISTS ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${IMAGE_REPO};"
    exit 1
fi
print_success "Image pushed successfully"

# -----------------------------------------------------------------------------
# STEP 4: GENERATE DEPLOYMENT SQL
# -----------------------------------------------------------------------------

print_header "Step 4: Deployment SQL"

SQL_FILE="$PROJECT_ROOT/deploy_generated.sql"

cat > "$SQL_FILE" << EOF
-- =============================================================================
-- Flux Data Forge - Auto-Generated Deployment SQL
-- Generated: $(date)
-- =============================================================================

-- Use your database and schema
USE DATABASE ${SNOWFLAKE_DATABASE};
USE SCHEMA ${SNOWFLAKE_SCHEMA};
USE WAREHOUSE ${SNOWFLAKE_WAREHOUSE};

-- Create image repository (if not exists)
CREATE IMAGE REPOSITORY IF NOT EXISTS ${IMAGE_REPO}
    COMMENT = 'Image repository for Flux Data Forge';

-- Create target table
CREATE TABLE IF NOT EXISTS AMI_STREAMING_READINGS (
    METER_ID VARCHAR(50) NOT NULL,
    READING_TIMESTAMP TIMESTAMP_NTZ NOT NULL,
    USAGE_KWH FLOAT,
    VOLTAGE FLOAT,
    POWER_FACTOR FLOAT,
    TEMPERATURE_C FLOAT,
    TRANSFORMER_ID VARCHAR(50),
    CIRCUIT_ID VARCHAR(50),
    SUBSTATION_ID VARCHAR(50),
    SERVICE_AREA VARCHAR(100),
    CUSTOMER_SEGMENT VARCHAR(50),
    LATITUDE FLOAT,
    LONGITUDE FLOAT,
    IS_OUTAGE BOOLEAN DEFAULT FALSE,
    DATA_QUALITY VARCHAR(20) DEFAULT 'VALID',
    EMISSION_PATTERN VARCHAR(50),
    PRODUCTION_MATCHED BOOLEAN DEFAULT FALSE,
    INGESTION_TIMESTAMP TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
)
CLUSTER BY (DATE_TRUNC('DAY', READING_TIMESTAMP), METER_ID)
DATA_RETENTION_TIME_IN_DAYS = 7
CHANGE_TRACKING = TRUE;

-- Create SPCS Service
CREATE SERVICE IF NOT EXISTS ${SERVICE_NAME}
    IN COMPUTE POOL ${COMPUTE_POOL}
    FROM SPECIFICATION \$\$
spec:
  containers:
    - name: flux-data-forge
      image: /${SNOWFLAKE_DATABASE}/${SNOWFLAKE_SCHEMA}/${IMAGE_REPO}/flux_data_forge:${IMAGE_TAG}
      env:
        SNOWFLAKE_DATABASE: ${SNOWFLAKE_DATABASE}
        SNOWFLAKE_SCHEMA: ${SNOWFLAKE_SCHEMA}
        SNOWFLAKE_WAREHOUSE: ${SNOWFLAKE_WAREHOUSE}
        SNOWFLAKE_ROLE: ${SNOWFLAKE_ROLE}
        AMI_TABLE: AMI_STREAMING_READINGS
      resources:
        requests:
          cpu: 1
          memory: 2Gi
        limits:
          cpu: 2
          memory: 4Gi
  endpoints:
    - name: app
      port: 8080
      public: true
\$\$
    COMMENT = 'Flux Data Forge - Synthetic AMI Data Generation Service';

-- Check service status
SELECT SYSTEM\$GET_SERVICE_STATUS('${SERVICE_NAME}');

-- Get service URL
SHOW ENDPOINTS IN SERVICE ${SERVICE_NAME};
EOF

print_success "Deployment SQL generated: $SQL_FILE"

# -----------------------------------------------------------------------------
# NEXT STEPS
# -----------------------------------------------------------------------------

print_header "Deployment Complete!"

echo ""
echo "Next steps:"
echo ""
echo "  1. Run the generated SQL in Snowflake Worksheets:"
echo "     $SQL_FILE"
echo ""
echo "  2. Wait for service to reach READY state:"
echo "     SELECT SYSTEM\$GET_SERVICE_STATUS('${SERVICE_NAME}');"
echo ""
echo "  3. Get the application URL:"
echo "     SHOW ENDPOINTS IN SERVICE ${SERVICE_NAME};"
echo ""
echo "  4. Open the ingress_url in your browser"
echo ""
print_success "Happy demo-ing!"
