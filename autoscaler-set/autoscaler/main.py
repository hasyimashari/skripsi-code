import os
import asyncio
import signal
import sys
import time
import csv
from typing import Dict
from datetime import datetime

from kubernetes import client, config
from kubernetes.client.rest import ApiException
from kubernetes_asyncio import client as async_client, config as async_config

from sklearn.preprocessing import MinMaxScaler

from helper.model_handler import ModelHandler, ValidationThresholds
from helper.prometheus_client import PrometheusClient, QueryConfig
from helper.scaling_algoirthm import ScalingAlgorithm, ScalingConfig


class AIHorizontalPodAutoscaler:
    def __init__(self, namespace: str):
        self.namespace = namespace
        self.running = False
        self.monitored_deployments: Dict[str, dict] = {}
        
        # Initialize components
        self.model_handler = None
        self.prometheus_client = None
        self.scaling_algorithm = ScalingAlgorithm()
        
        # Kubernetes clients
        self.apps_v1 = None
        self.custom_objects_api = None
        self.using_async_client = False
        
        # Shutdown flag
        self.shutdown_event = asyncio.Event()
        
        # Track last CRD reload time
        self.last_crd_reload = time.time()
        
        # CSV logging setup
        self.csv_filename = f"autoscaler_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        self.csv_data = []  # Store data in memory until shutdown

    def _log_to_csv(self, deployment_name: str, current_replicas: int, current_request: float = None, predicted_request: float = None):
        """Store deployment data in memory for later CSV export"""
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.csv_data.append([
                timestamp, 
                deployment_name, 
                current_replicas, 
                current_request if current_request is not None else 'N/A',
                predicted_request if predicted_request is not None else 'N/A'
            ])
        except Exception as e:
            print(f"Failed to store CSV data: {e}")

    def _write_csv_file(self):
        """Write all collected data to CSV file on shutdown"""
        try:
            with open(self.csv_filename, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['timestamp', 'deployment_name', 'current_replicas', 'current_request', 'predicted_request'])
                writer.writerows(self.csv_data)
            print(f"CSV file written: {self.csv_filename} with {len(self.csv_data)} records")
        except Exception as e:
            print(f"Failed to write CSV file: {e}")

    # init 
    async def initialize(self):
        try:
            # Initialize Kubernetes client
            await self._initialize_kubernetes()
            
            # Initialize AI model
            print("Loading AI model...")
            self.model_handler = ModelHandler("./autoscaler/model/bilstm_bo_opt/")
            print("AI model loaded successfully")
            
            # Initialize Prometheus client
            print("Initializing Prometheus client...")
            self.prometheus_client = PrometheusClient("http://192.168.49.2:30000/")
            print("Prometheus client initialized")
            
            # Load initial CRD configurations
            print("Loading CRD configurations...")
            await self._load_crd_configurations()
            print(f"Loaded {len(self.monitored_deployments)} CRD configurations")
            
        except Exception as e:
            raise Exception(f"Failed to initialize operator: {e}")
    
    async def _initialize_kubernetes(self):
        try:
            # Try async client first
            try:
                await async_config.load_incluster_config()
                print("Loaded in-cluster Kubernetes configuration (async)")
            except async_config.ConfigException:
                await async_config.load_kube_config()
                print("Loaded kubeconfig (async)")
            
            # Initialize async API clients
            self.apps_v1 = async_client.AppsV1Api()
            self.custom_objects_api = async_client.CustomObjectsApi()
            self.using_async_client = True
            print("Using async Kubernetes client")
            
        except Exception as async_error:
            # Fallback to sync clients if async not available
            print(f"Async client initialization failed: {async_error}")
            print("Falling back to sync Kubernetes client...")
            try:
                try:
                    config.load_incluster_config()
                    print("Loaded in-cluster Kubernetes configuration (sync)")
                except config.ConfigException:
                    config.load_kube_config()
                    print("Loaded kubeconfig (sync)")
                
                self.apps_v1 = client.AppsV1Api()
                self.custom_objects_api = client.CustomObjectsApi()
                self.using_async_client = False
                print("Using sync Kubernetes client")
            except Exception as sync_error:
                raise Exception(f"Failed to initialize Kubernetes client: async={async_error}, sync={sync_error}")

    # load CRD
    async def _load_crd_configurations(self):
        """Load CRD configurations with improved error handling"""
        try:
            # Get all AIHorizontalPodAutoscaler CRDs in the namespace
            if self.using_async_client:
                crds = await self.custom_objects_api.list_namespaced_custom_object(
                    group="aiautoscaler.io",
                    version="v1",
                    namespace=self.namespace,
                    plural="aihorizontalpodautoscalers"
                )
            else:
                # Sync client - run in executor to avoid blocking
                crds = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: self.custom_objects_api.list_namespaced_custom_object(
                        group="aiautoscaler.io",
                        version="v1",
                        namespace=self.namespace,
                        plural="aihorizontalpodautoscalers"
                    )
                )
            
            items = crds.get('items', [])
            if not items:
                print(f"WARNING: No AIHorizontalPodAutoscaler CRDs found in namespace '{self.namespace}'")
                print("Please ensure:")
                print(f"  1. The CRD is installed: kubectl get crd aihorizontalpodautoscalers.aiautoscaler.io")
                print(f"  2. CRD instances exist: kubectl get aihorizontalpodautoscalers -n {self.namespace}")
                return
            
            for crd in items:
                try:
                    await self._process_crd_configuration(crd)
                except Exception as crd_error:
                    crd_name = crd.get('metadata', {}).get('name', 'unknown')
                    print(f"WARNING: Failed to process CRD '{crd_name}': {crd_error}")
                    continue
            
        except ApiException as e:
            if e.status == 404:
                error_msg = (
                    f"CRD 'aihorizontalpodautoscalers.aiautoscaler.io' not found in cluster.\n"
                    f"Please install the CRD first:\n"
                    f"  kubectl apply -f <your-crd-definition>.yaml\n"
                    f"Then create CRD instances in namespace '{self.namespace}':\n"
                    f"  kubectl apply -f <your-aihpa-instance>.yaml -n {self.namespace}"
                )
                raise Exception(error_msg)
            else:
                raise Exception(f"Kubernetes API error (status {e.status}): {e.reason}")
        except Exception as e:
            raise Exception(f"Unexpected error loading CRD configurations: {e}")

    async def _reload_crd_configurations(self):
        """Reload CRD configurations with error handling"""
        try:
            print("Reloading CRD configurations...")
            await self._load_crd_configurations()
            print(f"Successfully reloaded {len(self.monitored_deployments)} CRD configurations")
        except Exception as e:
            print(f"WARNING: Failed to reload CRD configurations: {e}")
            # Don't raise - keep using existing configurations

    async def _process_crd_configuration(self, crd: dict):
        """Process a single CRD configuration with validation"""
        try:
            spec = crd.get('spec', {})
            metadata = crd.get('metadata', {})
            crd_name = metadata.get('name', 'unknown')
            
            # Validate required fields
            deployment_name = spec.get('targetDeployment')
            if not deployment_name:
                raise ValueError(f"Missing required field 'targetDeployment'")
            
            # Extract and validate thresholds
            validation_thresholds = spec.get('validationThresholds', {})
            thresholds = ValidationThresholds(
                max_historical_multiplier=validation_thresholds.get('maxHistoricalMultiplier', 2.0),
                max_spike_multiplier=validation_thresholds.get('maxSpikeMultiplier', 3.0)
            )

            # Extract and validate Prometheus config
            prometheus_config = spec.get('prometheusConfig', {})
            if not spec.get('prometheusService'):
                raise ValueError(f"Missing required field 'prometheusService'")
            
            query_config = QueryConfig(
                service_name=spec.get('prometheusService'),
                window_minutes=prometheus_config.get('windowMinute', 10),
                query_template=prometheus_config.get('queryTemplate', 'rate(http_requests_total{{service="{service_name}"}}[{window}m])')
            )

            # Extract and validate scaling config
            scaling_cfg = spec.get('scalingConfig', {})
            scaling_config = ScalingConfig(
                min_replicas=scaling_cfg.get('minReplicas', 1),
                max_replicas=scaling_cfg.get('maxReplicas', 10),
                workload_per_pod=scaling_cfg.get('workloadPerPod', 100),
                resource_removal_strategy=scaling_cfg.get('resourceRemovalStrategy', 'gradual'),
                cooldown_period=scaling_cfg.get('cooldownPeriod', 300)
            )

            print(f"Loaded CRD '{crd_name}' for deployment '{deployment_name}'")
            
            # Store configuration
            self.monitored_deployments[deployment_name] = {
                'crd_name': crd_name,
                'thresholds': thresholds,
                'query_config': query_config,
                'scaling_config': scaling_config,
                'last_processed': None,
                'error_count': 0
            }
            
        except (ValueError, KeyError) as e:
            raise Exception(f"Invalid CRD configuration: {e}")
        except Exception as e:
            raise Exception(f"Error processing CRD configuration: {e}")
    
    async def _get_current_replicas(self, deployment_name: str):
        """Get current replica count with error handling"""
        try:
            if self.using_async_client:
                deployment = await self.apps_v1.read_namespaced_deployment(
                    name=deployment_name,
                    namespace=self.namespace
                )
            else:
                # Sync client - run in executor
                deployment = await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: self.apps_v1.read_namespaced_deployment(
                        name=deployment_name,
                        namespace=self.namespace
                    )
                )
            return deployment.status.ready_replicas or 0
            
        except ApiException as e:
            if e.status == 404:
                print(f"WARNING: Deployment '{deployment_name}' not found in namespace '{self.namespace}'")
            else:
                print(f"API error getting replicas for {deployment_name} (status {e.status}): {e.reason}")
            return None
        except Exception as e:
            print(f"Unexpected error getting replicas for {deployment_name}: {e}")
            return None
    
    async def _get_historical_metrics(self, query_config: QueryConfig):
        """Get historical metrics with error handling"""
        try:
            metrics = self.prometheus_client.get_historical_workload(query_config)
            
            if not metrics:
                print(f"WARNING: No metrics returned from Prometheus for service '{query_config.service_name}'")
                return None
            
            return metrics
            
        except Exception as e:
            print(f"Error getting historical metrics for '{query_config.service_name}': {e}")
            return None
    
    async def _make_prediction(self, historical_data: list, thresholds: ValidationThresholds):
        """Make prediction with data validation"""
        try:
            # Ensure we have exactly 10 data points
            if len(historical_data) != 10:
                # Pad or truncate as needed
                if len(historical_data) < 10:
                    # Pad with last known value or zero
                    last_value = historical_data[-1] if historical_data else 0
                    historical_data.extend([last_value] * (10 - len(historical_data)))
                    print(f"  - Padded historical data to 10 points (was {len(historical_data)})")
                else:
                    # Take last 10 points
                    historical_data = historical_data[-10:]
                    print(f"  - Truncated historical data to 10 points (was {len(historical_data)})")

            scaler = MinMaxScaler(feature_range=(-1, 1))
            
            # Make prediction
            prediction = self.model_handler.predict(historical_data, scaler, thresholds)
            
            return prediction
            
        except Exception as e:
            print(f"Error making prediction: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    async def _execute_scaling(self, deployment_name: str, scaling_decision):
        """Execute scaling action with error handling"""
        try:
            # Update deployment replicas
            body = {
                'spec': {
                    'replicas': scaling_decision.target_replicas
                }
            }
            
            if self.using_async_client:
                await self.apps_v1.patch_namespaced_deployment(
                    name=deployment_name,
                    namespace=self.namespace,
                    body=body
                )
            else:
                # Sync client - run in executor
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: self.apps_v1.patch_namespaced_deployment(
                        name=deployment_name,
                        namespace=self.namespace,
                        body=body
                    )
                )
            
            print(f"  ✓ Successfully scaled {deployment_name} to {scaling_decision.target_replicas} replicas")
            return True
            
        except ApiException as e:
            if e.status == 404:
                print(f"  ✗ Deployment '{deployment_name}' not found")
            elif e.status == 422:
                print(f"  ✗ Invalid replica count: {e.reason}")
            else:
                print(f"  ✗ API error scaling deployment {deployment_name} (status {e.status}): {e.reason}")
            return False
        except Exception as e:
            print(f"  ✗ Unexpected error scaling deployment {deployment_name}: {e}")
            return False

    async def _process_deployment(self, deployment_name: str, config: dict):
        """Process a single deployment with comprehensive error handling"""
        try:
            print(f"\n{'='*60}")
            print(f"Processing deployment: {deployment_name}")
            print(f"{'='*60}")
            
            # Get current deployment status
            current_replicas = await self._get_current_replicas(deployment_name)
            if current_replicas is None:
                print(f"  ✗ Failed to get current replicas for {deployment_name}")
                config['error_count'] += 1
                return
            
            print(f"  • Current replicas: {current_replicas}")
            
            # Get historical metrics from Prometheus
            historical_data = await self._get_historical_metrics(config['query_config'])
            
            if not historical_data:
                print(f"  ✗ No historical data available for {deployment_name}")
                # Log with basic info if no historical data
                self._log_to_csv(deployment_name, current_replicas)
                config['error_count'] += 1
                return
            
            print(f"  • Historical data points: {len(historical_data)}")
            
            # Extract current request (last value from historical data)
            current_request = historical_data[-1] if historical_data else None
            current_request_value = current_request[0] if current_request else None
            print(f"  • Current request rate: {current_request_value}")
            
            # Make AI prediction
            predicted_workload = await self._make_prediction(historical_data, config['thresholds'])
            
            if predicted_workload is None:
                print(f"  ✗ Failed to make prediction for {deployment_name}")
                # Still log the current state
                self._log_to_csv(deployment_name, current_replicas, current_request_value)
                config['error_count'] += 1
                return
            
            print(f"  • Predicted workload: {predicted_workload:.2f}")
            
            # Log current state to CSV with request information
            self._log_to_csv(deployment_name, current_replicas, current_request_value, predicted_workload)
            
            # Calculate scaling decision
            scaling_decision = self.scaling_algorithm.calculate_scaling_decision(
                deployment_name,
                predicted_workload,
                current_replicas,
                config['scaling_config']
            )
            
            print(f"  • Scaling decision: {scaling_decision.action}")
            print(f"  • Target replicas: {scaling_decision.target_replicas}")
            print(f"  • Reason: {scaling_decision.reason}")
            
            # Execute scaling if needed
            if scaling_decision.action in ["scale_out", "scale_in"]:
                print(f"  → Executing scaling action: {scaling_decision.action}")
                success = await self._execute_scaling(deployment_name, scaling_decision)
                if success:
                    self.scaling_algorithm.execute_scaling_decision(deployment_name, scaling_decision)
                    # Reset error count on successful scaling
                    config['error_count'] = 0
                else:
                    config['error_count'] += 1
            else:
                print(f"  → No scaling action needed")
                # Reset error count on successful processing even if no scaling
                config['error_count'] = 0
            
            config['last_processed'] = datetime.now()
            
        except Exception as e:
            print(f"  ✗ Error processing deployment {deployment_name}: {e}")
            import traceback
            traceback.print_exc()
            config['error_count'] += 1

    def _setup_signal_handlers(self):
        """Setup signal handlers for graceful shutdown"""
        def signal_handler(signum, frame):
            print(f"\n{'='*60}")
            print(f"Received signal {signum}, initiating graceful shutdown...")
            print(f"{'='*60}")
            self.running = False
            # Schedule shutdown in the event loop
            try:
                loop = asyncio.get_running_loop()
                loop.call_soon_threadsafe(self.shutdown_event.set)
            except RuntimeError:
                # No running loop, set directly
                self.shutdown_event.set()
        
        # Only set up signal handlers if we're the main thread
        if hasattr(signal, 'SIGTERM'):
            signal.signal(signal.SIGTERM, signal_handler)
        if hasattr(signal, 'SIGINT'):
            signal.signal(signal.SIGINT, signal_handler)

    async def run(self):
        """Main operation loop with error handling"""
        self.running = True
        
        # Set up signal handlers for graceful shutdown
        self._setup_signal_handlers()
        
        try:
            loop_count = 0
            while self.running and not self.shutdown_event.is_set():
                loop_count += 1
                loop_start_time = time.time()
                
                print(f"\n{'#'*60}")
                print(f"# Loop {loop_count} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"# Monitoring {len(self.monitored_deployments)} deployments")
                print(f"{'#'*60}")
                
                # Process each monitored deployment
                # Create a copy of keys to avoid modification during iteration
                deployment_names = list(self.monitored_deployments.keys())
                
                if not deployment_names:
                    print("WARNING: No deployments to monitor. Waiting for CRD configurations...")
                
                for deployment_name in deployment_names:
                    if deployment_name not in self.monitored_deployments:
                        continue  # Skip if already removed
                        
                    config = self.monitored_deployments[deployment_name]
                    
                    try:
                        await self._process_deployment(deployment_name, config)
                    except Exception as e:
                        print(f"✗ Critical error processing deployment {deployment_name}: {e}")
                        import traceback
                        traceback.print_exc()
                        config['error_count'] += 1
                    
                    # Remove deployment if too many consecutive errors
                    if config['error_count'] > 10:
                        print(f"\n{'!'*60}")
                        print(f"! WARNING: Removing deployment '{deployment_name}' from monitoring")
                        print(f"! Reason: {config['error_count']} consecutive errors")
                        print(f"{'!'*60}\n")
                        del self.monitored_deployments[deployment_name]
                
                # Reload CRD configurations periodically (every 10 minutes)
                current_time = time.time()
                if current_time - self.last_crd_reload >= 600:  # 10 minutes
                    try:
                        await self._reload_crd_configurations()
                        self.last_crd_reload = current_time
                    except Exception as e:
                        print(f"WARNING: Failed to reload CRD configurations: {e}")
                
                # Calculate sleep time to maintain 1-minute intervals
                loop_duration = time.time() - loop_start_time
                sleep_time = max(0, 60 - loop_duration)  # 1-minute cycle
                
                print(f"\nLoop completed in {loop_duration:.2f}s, sleeping for {sleep_time:.2f}s...")
                
                if sleep_time > 0:
                    try:
                        await asyncio.wait_for(self.shutdown_event.wait(), timeout=sleep_time)
                        break  # Shutdown requested
                    except asyncio.TimeoutError:
                        pass  # Normal timeout, continue loop
                
        except Exception as e:
            print(f"\n{'!'*60}")
            print(f"! CRITICAL ERROR in main loop: {e}")
            print(f"{'!'*60}\n")
            import traceback
            traceback.print_exc()
            raise

    async def shutdown(self):
        """Graceful shutdown with proper cleanup"""
        print("\n" + "="*60)
        print("Shutting down AIHorizontalPodAutoscaler...")
        print("="*60)
        
        self.running = False
        self.shutdown_event.set()
        
        # Write CSV file before shutdown
        try:
            print("Writing CSV log file...")
            self._write_csv_file()
        except Exception as e:
            print(f"Error writing CSV file: {e}")
        
        # Close Kubernetes API clients properly
        try:
            if self.using_async_client:
                print("Closing async Kubernetes API clients...")
                if self.apps_v1 and hasattr(self.apps_v1, 'api_client'):
                    await self.apps_v1.api_client.close()
                if self.custom_objects_api and hasattr(self.custom_objects_api, 'api_client'):
                    await self.custom_objects_api.api_client.close()
                print("Async clients closed successfully")
        except Exception as e:
            print(f"Warning: Error closing API clients: {e}")
        
        print("Shutdown complete")

# Main entry point
async def main():
    namespace = "test-autoscaler"
    operator = None
    
    print("="*60)
    print("AIHorizontalPodAutoscaler Starting")
    print(f"Namespace: {namespace}")
    print(f"Time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60 + "\n")
    
    # Create operator
    operator = AIHorizontalPodAutoscaler(namespace=namespace)
    
    try:
        print("Initializing operator...")
        await operator.initialize()
        print("\n✓ Operator initialized successfully\n")
        
        print("Starting main monitoring loop...")
        await operator.run()
    except KeyboardInterrupt:
        print("\nReceived keyboard interrupt")
    except Exception as e:
        print("\n" + "!"*60)
        print(f"! Operator failed: {e}")
        print("!"*60 + "\n")
        import traceback
        traceback.print_exc()
    finally:
        # ALWAYS ensure shutdown is called
        if operator:
            print("\nCleaning up...")
            try:
                await operator.shutdown()
            except Exception as cleanup_error:
                print(f"Error during cleanup: {cleanup_error}")
                # Still try to write CSV even if other cleanup fails
                try:
                    operator._write_csv_file()
                except Exception as csv_error:
                    print(f"Error writing CSV during emergency cleanup: {csv_error}")

if __name__ == "__main__":
    exit_code = 0
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nReceived keyboard interrupt, shutting down...")
        exit_code = 0
    except Exception as e:
        print(f"\nFatal error: {e}")
        import traceback
        traceback.print_exc()
        exit_code = 1
    finally:
        print("\nExiting...")
        sys.exit(exit_code)