#!/bin/bash

set -e

echo "test-app deployment..."

kubectl apply -f ../kube-manifest/test-app-ns.yaml

echo "Applying configuration..."
kubectl apply -f ../kube-manifest/test-app-deployment.yaml
kubectl apply -f ../kube-manifest/test-app-service.yaml

echo "Waiting for test-app deployment to be ready..."
kubectl wait --for=condition=available --timeout=300s deployment.apps/test-app -n test-autoscaler

# Get Minikube IP
MINIKUBE_IP=$(minikube ip)

echo ""
echo "test-app:"
echo "  1. Via NodePort: http://$MINIKUBE_IP:30500"
echo ""