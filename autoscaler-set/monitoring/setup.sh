#!/bin/bash

set -e

echo "Setting up Prometheus + Grafana for Minikube..."

# Check if minikube is running
if ! minikube status > /dev/null 2>&1; then
    echo "Minikube is not running. Please start minikube first:"
    echo "minikube start"
    exit 1
fi

# Enable metrics-server addon for Minikube
echo "Enabling metrics-server addon..."
minikube addons enable metrics-server

kubectl apply -f monitoring-ns.yaml

# Apply configuration
echo "Applying Prometheus configuration..."
kubectl apply -f ./prometheus/

echo "Applying kube-state configuration..."
kubectl apply -f ./kube-state/cluster-role-binding.yaml
kubectl apply -f ./kube-state/cluster-role.yaml
kubectl apply -f ./kube-state/deployment.yaml
kubectl apply -f ./kube-state/service-account.yaml
kubectl apply -f ./kube-state/service.yaml
kubectl apply -k ./kube-state/

echo "Applying node-exporter configuration..."
kubectl apply -f ./node-exporter/

echo "Applying Grafana configuration..."
kubectl apply -f ./grafana/

# Wait for deployments to be ready
echo "Waiting for Prometheus to be ready..."
kubectl wait --for=condition=available --timeout=300s deployment/prometheus-deployment -n monitoring

echo "Waiting for Grafana to be ready..."
kubectl wait --for=condition=available --timeout=300s deployment/grafana -n monitoring

# Get Minikube IP
MINIKUBE_IP=$(minikube ip)

echo ""
echo "Prometheus + Grafana setup complete!"
echo ""
echo "Access Prometheus:"
echo "  1. Via NodePort: http://$MINIKUBE_IP:30000"
echo ""
echo "Access Grafana:"
echo "  1. Via NodePort: http://$MINIKUBE_IP:32000"
echo ""