#!/bin/bash

set -e

echo "setting up manifest..."

if ! minikube status > /dev/null 2>&1; then
    echo "minikube is not running, please start minikube first"
    exit 1
fi

minikube addons enable metrics-server

echo ""
echo "check and load image to minikube..."
echo ""

load_images_if_missing() {
    local images=("$@")

    for image in "${images[@]}"; do
        if minikube image ls | grep -q "$image"; then
            echo "'$image' already loaded, skip proccess"
        else
            echo "loaded '$image' "
            minikube image load "$image"
        fi
    done
}

IMAGES=(
    "prom/prometheus:latest"
    "grafana/grafana:latest"
    "test-app:latest"
)

load_images_if_missing "${IMAGES[@]}"

echo ""
echo "applying monitoring configuration..."
echo ""

kubectl apply -f ./monitoring/monitoring-ns.yaml

kubectl apply -f ./monitoring/prometheus/
kubectl apply -k ./monitoring/kube-state/
kubectl apply -f ./monitoring/node-exporter/
kubectl apply -f ./monitoring/grafana/

echo ""
echo "waiting for prometheus and grafana to be ready..."
echo ""

kubectl wait --for=condition=available --timeout=300s deployment/prometheus-deployment -n monitoring
kubectl wait --for=condition=available --timeout=300s deployment/grafana -n monitoring

echo ""
echo "applying test-app deployment"
echo ""

kubectl apply -f ./test-app/kube-manifest/test-app-ns.yaml
kubectl apply -f ./test-app/kube-manifest/test-app-deployment.yaml
kubectl apply -f ./test-app/kube-manifest/test-app-service.yaml

echo ""
echo "waiting for test-app deployment to be ready..."
echo ""

kubectl wait --for=condition=available --timeout=300s deployment.apps/test-app -n test-autoscaler

echo ""
echo "applying crd manifest"
echo ""

kubectl apply -f ./crd/manifests/ai-hpa-crd.yaml
kubectl apply -f ./crd/manifests/ai-hpa-cr.yaml

MINIKUBE_IP=$(minikube ip)

echo ""
echo "access prometheus:"
echo " NodePort: http://$MINIKUBE_IP:30000"
echo ""
echo "access grafana:"
echo " NodePort: http://$MINIKUBE_IP:32000"
echo ""
echo "test-app:"
echo " NodePort: http://$MINIKUBE_IP:30500"
echo ""