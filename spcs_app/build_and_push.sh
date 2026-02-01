#!/bin/bash
# =============================================================================
# Flux Data Forge - Build and Push Docker Image to Snowflake Image Registry
# =============================================================================

# Configuration - UPDATE THESE VALUES
REGISTRY_URL="<YOUR_ORG>-<YOUR_ACCOUNT>.registry.snowflakecomputing.com"
DATABASE="<YOUR_DATABASE>"
SCHEMA="<YOUR_SCHEMA>"
IMAGE_REPO="<YOUR_IMAGE_REPO>"
IMAGE_NAME="flux_data_forge"
TAG="${1:-latest}"

# Full image path
FULL_IMAGE="${REGISTRY_URL}/${DATABASE}/${SCHEMA}/${IMAGE_REPO}/${IMAGE_NAME}:${TAG}"

echo "============================================="
echo "Building Flux Data Forge Docker Image"
echo "============================================="
echo "Registry: ${REGISTRY_URL}"
echo "Image: ${FULL_IMAGE}"
echo ""

# Login to Snowflake registry
echo "Logging in to Snowflake registry..."
docker login ${REGISTRY_URL}

# Build the image
echo "Building Docker image..."
docker build -t ${IMAGE_NAME}:${TAG} -f Dockerfile ..

# Tag for Snowflake registry
echo "Tagging image for Snowflake registry..."
docker tag ${IMAGE_NAME}:${TAG} ${FULL_IMAGE}

# Push to registry
echo "Pushing to Snowflake registry..."
docker push ${FULL_IMAGE}

echo ""
echo "============================================="
echo "Build complete!"
echo "============================================="
echo "Image pushed: ${FULL_IMAGE}"
echo ""
echo "Next steps:"
echo "1. Run deploy_spcs.sql to create the SPCS service"
echo "2. Update service_spec.yaml with your image path"
