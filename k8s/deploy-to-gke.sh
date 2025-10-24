#!/bin/bash
set -e

# GKE Deployment Script for AMP ERC20 Loader
# This script automates the deployment of the ERC20 loader to Google Kubernetes Engine

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration (override with environment variables)
PROJECT_ID="${GCP_PROJECT_ID:-}"
CLUSTER_NAME="${GKE_CLUSTER_NAME:-staging}"
REGION="${GKE_REGION:-us-central1}"
ZONE="${GKE_ZONE:-us-central1-a}"
NODE_MACHINE_TYPE="${GKE_MACHINE_TYPE:-n1-highmem-4}"
NUM_NODES="${GKE_NUM_NODES:-1}"

# Function to print colored output
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Function to check if command exists
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

# Check prerequisites
print_info "Checking prerequisites..."

if ! command_exists gcloud; then
    print_error "gcloud CLI is not installed. Please install it from https://cloud.google.com/sdk/docs/install"
    exit 1
fi

if ! command_exists kubectl; then
    print_error "kubectl is not installed. Please install it: gcloud components install kubectl"
    exit 1
fi

# Get or prompt for project ID
if [ -z "$PROJECT_ID" ]; then
    print_warning "GCP_PROJECT_ID not set. Attempting to get from gcloud config..."
    PROJECT_ID=$(gcloud config get-value project 2>/dev/null)

    if [ -z "$PROJECT_ID" ]; then
        print_error "Could not determine GCP project ID."
        echo "Please set it with: export GCP_PROJECT_ID=your-project-id"
        exit 1
    fi
fi

print_info "Using GCP Project: $PROJECT_ID"
print_info "Using Cluster: $CLUSTER_NAME in $REGION"

# SAFETY CHECK: Ensure project contains "staging"
if [[ ! "$PROJECT_ID" =~ staging ]]; then
    print_error "SAFETY CHECK FAILED!"
    print_error "Project ID must contain 'staging' for deployment safety."
    print_error "Current project: $PROJECT_ID"
    echo ""
    print_error "This prevents accidental deployment to production."
    print_error "If you need to deploy to a different environment, update this script."
    exit 1
fi

print_info "Safety check passed: Project contains 'staging'"

# Set the project
gcloud config set project "$PROJECT_ID"

# Check if cluster exists
print_info "Checking if GKE cluster exists..."
if gcloud container clusters describe "$CLUSTER_NAME" --region="$REGION" >/dev/null 2>&1; then
    print_info "Cluster '$CLUSTER_NAME' already exists. Using existing cluster."
else
    print_warning "Cluster '$CLUSTER_NAME' does not exist."
    read -p "Do you want to create it? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        print_info "Creating GKE cluster '$CLUSTER_NAME'..."
        gcloud container clusters create "$CLUSTER_NAME" \
            --region="$REGION" \
            --machine-type="$NODE_MACHINE_TYPE" \
            --num-nodes="$NUM_NODES" \
            --enable-autoscaling \
            --min-nodes=1 \
            --max-nodes=3 \
            --enable-autorepair \
            --enable-autoupgrade \
            --disk-size=50 \
            --disk-type=pd-standard

        print_info "Cluster created successfully!"
    else
        print_error "Cannot proceed without a cluster. Exiting."
        exit 1
    fi
fi

# Get cluster credentials
print_info "Getting cluster credentials..."
gcloud container clusters get-credentials "$CLUSTER_NAME" --region="$REGION"

# Create namespace if it doesn't exist
#print_info "Ensuring namespace exists..."
#kubectl create namespace amp-loader --dry-run=client -o yaml | kubectl apply -f -

# Set current context to namespace
kubectl config set-context --current --namespace=nozzle

# Create secrets from .env file if it exists
if [ -f "../.test.env" ]; then
    print_info "Creating Kubernetes secrets from .env file..."

    # Source the .env file
    export $(cat ../.env | grep -v '^#' | xargs)

    # Create the amp-secrets secret
    kubectl create secret generic amp-secrets \
        --from-literal=amp-server-url="$AMP_SERVER_GOOGLE_CLOUD_URL" \
        --from-literal=snowflake-account="$SNOWFLAKE_ACCOUNT" \
        --from-literal=snowflake-user="$SNOWFLAKE_USER" \
        --from-literal=snowflake-warehouse="$SNOWFLAKE_WAREHOUSE" \
        --from-literal=snowflake-database="$SNOWFLAKE_DATABASE" \
        --from-literal=snowflake-private-key="$SNOWFLAKE_PRIVATE_KEY" \
        --dry-run=client -o yaml | kubectl apply -f -

    print_info "Secrets created successfully!"
else
    print_warning ".env file not found. You'll need to create secrets manually."
    print_info "You can use: kubectl apply -f k8s/secret.yaml"
fi

# Create GitHub Container Registry secret if credentials are available
#if [ -n "$GITHUB_USERNAME" ] && [ -n "$GITHUB_PAT" ]; then
#    print_info "Creating GitHub Container Registry secret..."
#    kubectl create secret docker-registry ghcr-secret \
#        --docker-server=ghcr.io \
#        --docker-username="$GITHUB_USERNAME" \
#        --docker-password="$GITHUB_PAT" \
#        --docker-email="${GITHUB_EMAIL:-noreply@github.com}" \
#        --dry-run=client -o yaml | kubectl apply -f -
#
#    print_info "GHCR secret created successfully!"
#else
#    print_warning "GitHub credentials not found (GITHUB_USERNAME, GITHUB_PAT)."
#    print_info "If using a private registry, create the secret manually:"
#    print_info "  kubectl create secret docker-registry ghcr-secret \\"
#    print_info "    --docker-server=ghcr.io \\"
#    print_info "    --docker-username=YOUR_USERNAME \\"
#    print_info "    --docker-password=YOUR_PAT"
#fi

# Apply Kubernetes manifests
print_info "Applying Kubernetes deployment..."
kubectl apply -f deployment.yaml

# Wait for deployment to be ready
print_info "Waiting for deployment to be ready..."
kubectl wait --for=condition=available --timeout=300s deployment/amp-erc20-loader

# Get deployment status
print_info "Deployment status:"
kubectl get deployments
kubectl get pods

# Show logs from the first pod
print_info "Fetching logs from the loader..."
POD_NAME=$(kubectl get pods -l app=amp-erc20-loader -o jsonpath='{.items[0].metadata.name}')
echo ""
print_info "Pod name: $POD_NAME"
echo ""
print_info "Recent logs (last 50 lines):"
kubectl logs "$POD_NAME" --tail=50

echo ""
print_info "Deployment completed successfully!"
echo ""
print_info "Useful commands:"
echo "  - View logs:        kubectl logs -f deployment/amp-erc20-loader"
echo "  - Get pod status:   kubectl get pods -l app=amp-erc20-loader"
echo "  - Describe pod:     kubectl describe pod $POD_NAME"
echo "  - Delete deployment: kubectl delete -f k8s/deployment.yaml"
echo "  - Scale deployment:  kubectl scale deployment/amp-erc20-loader --replicas=2"
echo ""