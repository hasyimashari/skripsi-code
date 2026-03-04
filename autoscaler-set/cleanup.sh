#!/bin/bash

set -e

echo ""
echo "cleaning up manifest..."
echo ""

kubectl get serviceaccount,deployment,service -n kube-system | grep kube-state-metrics || echo "   No kube-state-metrics resources found in kube-system"
kubectl get clusterrole,clusterrolebinding | grep kube-state-metrics || echo "   No kube-state-metrics RBAC resources found"

safe_delete() {
    local resource_type=$1
    local resource_name=$2
    local namespace=${3:-""}
    
    if [ -n "$namespace" ]; then
        if kubectl get $resource_type $resource_name -n $namespace >/dev/null 2>&1; then
            echo "found and deleting $resource_type/$resource_name in namespace $namespace"
            kubectl delete $resource_type $resource_name -n $namespace
        else
            echo "$resource_type/$resource_name not found in namespace $namespace"
        fi
    else
        if kubectl get $resource_type $resource_name >/dev/null 2>&1; then
            echo "found and deleting $resource_type/$resource_name"
            kubectl delete $resource_type $resource_name
        else
            echo "$resource_type/$resource_name not found (cluster-wide)"
        fi
    fi
}


echo ""
echo "cleaning up monitoring configuration"
echo ""

safe_delete "service" "grafana" "monitoring"
safe_delete "deployment" "grafana" "monitoring"
safe_delete "configmap" "grafana-datasources" "monitoring"

safe_delete "service" "node-exporter" "monitoring"
safe_delete "daemonset" "node-exporter" "monitoring"

safe_delete "service" "kube-state-metrics" "kube-system"
safe_delete "deployment" "kube-state-metrics" "kube-system"
safe_delete "serviceaccount" "kube-state-metrics" "kube-system"
safe_delete "clusterrole" "kube-state-metrics"
safe_delete "clusterrolebinding" "kube-state-metrics"

safe_delete "service" "prometheus-service" "monitoring"
safe_delete "deployment" "prometheus-deployment" "monitoring"
safe_delete "configmap" "prometheus-server-conf" "monitoring"

safe_delete "clusterrolebinding" "prometheus"
safe_delete "clusterrole" "prometheus"

if kubectl get namespace monitoring >/dev/null 2>&1; then
    echo "   deleting namespace monitoring (this may take a moment...)"
    kubectl delete namespace monitoring --timeout=60s
fi


echo ""
echo "cleaning up crd configuration"
echo ""

safe_delete "AIHorizontalPodAutoscaler" "test-autoscaler-app" "test-autoscaler"
safe_delete "CustomResourceDefinition" "aihorizontalpodautoscalers.aiautoscaler.io"


echo ""
echo "cleaning up test-app deployment"
echo ""

safe_delete "service" "test-app-service" "test-autoscaler"
safe_delete "deployment" "test-app" "test-autoscaler"

echo "Removing test-autoscaler namespace..."
if kubectl get namespace test-autoscaler >/dev/null 2>&1; then
    echo "Deleting namespace test-autoscaler"
    kubectl delete namespace test-autoscaler --timeout=60s
fi

echo ""
echo "cleanup completed"
echo ""