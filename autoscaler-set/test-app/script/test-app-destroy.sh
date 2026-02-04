#!/bin/bash

# Cleanup Script for Prometheus + Grafana Setup
# This script removes all Prometheus and Grafana resources from Minikube

set -e

echo "Cleaning test-app resource"

# Function to check if resource exists before deleting
safe_delete() {
    local resource_type=$1
    local resource_name=$2
    local namespace=${3:-""}
    
    if [ -n "$namespace" ]; then
        if kubectl get $resource_type $resource_name -n $namespace >/dev/null 2>&1; then
            echo "Found and deleting $resource_type/$resource_name in namespace $namespace"
            kubectl delete $resource_type $resource_name -n $namespace
        else
            echo "$resource_type/$resource_name not found in namespace $namespace"
        fi
    else
        if kubectl get $resource_type $resource_name >/dev/null 2>&1; then
            echo "Found and deleting $resource_type/$resource_name"
            kubectl delete $resource_type $resource_name
        else
            echo "$resource_type/$resource_name not found (cluster-wide)"
        fi
    fi
}

safe_delete "service" "test-app-service" "test-autoscaler"
safe_delete "deployment" "test-app" "test-autoscaler"

echo "Removing test-autoscaler namespace..."
if kubectl get namespace test-autoscaler >/dev/null 2>&1; then
    echo "Deleting namespace test-autoscaler"
    kubectl delete namespace test-autoscaler --timeout=60s
fi

echo ""
echo "Cleanup completed!"
echo ""
echo "To redeploy, run the setup script again."