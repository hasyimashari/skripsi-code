import os
import asyncio
import signal
import sys
import time
import csv
import logging
from datetime import datetime

from dotenv import load_dotenv

load_dotenv()

from kubernetes import client, config
from kubernetes.client.rest import ApiException
from kubernetes_asyncio import client as async_client, config as async_config

from sklearn.preprocessing import MinMaxScaler

from helper.model_handler import ModelHandler, ValidationThresholds
from helper.prometheus_client import PrometheusClient, QueryConfig
from helper.scaling_algoirthm import ScalingAlgorithm, ScalingConfig



logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - main_crd - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('crd.log')
    ]
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)



class AIHorizontalPodAutoscaler:
    def __init__(self, namespace: str):
        self.namespace = namespace
        self.running = False
        self.monitored_deployments = {}
        
        self.model_handler  = None
        self.prometheus_client = None
        self.scaling_algorithm = ScalingAlgorithm()
        
        self.apps_v1 = None
        self.custom_objects_api = None
        self.using_async_client = False
        
        self.shutdown_event = asyncio.Event()
        
        self.last_crd_reload = time.time()
        
        self.csv_dir = "./autoscaler-data"
        csv_filename = f"autoscaler_log_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        self.csv_file = os.path.join(self.csv_dir, csv_filename)
        
        self.csv_temp_data = []

    def _log_to_csv(self, deployment_name: str, current_replicas: int, current_request: float = None, predicted_request: float = None):
        # Store deployment data in memory for later CSV export
        try:
            timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            self.csv_temp_data.append([
                timestamp, 
                deployment_name, 
                current_replicas, 
                current_request if current_request is not None else 'N/A',
                predicted_request if predicted_request is not None else 'N/A'
            ])
        except Exception as e:
            logger.error(f"[tracker] failed to save data : {e}")

    def _write_csv_file(self):
        # Write all collected data to CSV file on shutdown
        os.makedirs(self.csv_dir, exist_ok=True)
        
        try:
            with open(self.csv_file, 'w', newline='') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['timestamp', 'deployment_name', 'current_replicas', 'current_request', 'predicted_request'])
                writer.writerows(self.csv_temp_data)
                
            print(f"csf file written into {self.csv_file}")    
            logger.info(f"csf file written into {self.csv_file}")
        except Exception as e:
            print(f"failed to write csv : {e}")
            logger.error(f"failed to write csv : {e}")

    async def initialize(self):
        try:
            await self._initialize_kubernetes()
            
            logger.info("load ai model")
            self.model_handler = ModelHandler(os.getenv('USED_MODEL_PATH'))
            logger.info("ai model loaded")
            
            logger.info("initialize prometheus client")
            self.prometheus_client = PrometheusClient(os.getenv('PROMETHEUS_URL'))
            logger.info("prometheus client initialized")
            
            logger.info("load crd config")
            await self._load_crd_configurations()
            logger.info("crd config loaded")
            
        except Exception as e:
            logger.error(f"failed to initialize operator : {e}")
    
    async def _initialize_kubernetes(self):
        try:
            try:
                await async_config.load_incluster_config()
                logger.info("load in-cluster k8s config (async)")
            except async_config.ConfigException:
                await async_config.load_kube_config()
                logger.info("k8s config loaded (async)")
            
            self.apps_v1 = async_client.AppsV1Api()
            self.custom_objects_api = async_client.CustomObjectsApi()
            self.using_async_client = True
            logger.info("use async k8s client")
            
        except Exception as async_error:
            logger.warning(f"async client init fail: {async_error}")
            logger.warning("falling back sync k8s client")
            
            try:
                try:
                    config.load_incluster_config()
                except config.ConfigException:
                    config.load_kube_config()
                
                self.apps_v1 = client.AppsV1Api()
                self.custom_objects_api = client.CustomObjectsApi()
                self.using_async_client = False
                
                logger.info("use k8s client")
            except Exception as sync_error:
                logger.error(f"failed to initialize k8s client: async={async_error}, sync={sync_error}")

    async def _load_crd_configurations(self):
        # Load CRD configurations with improved error handling
        try:
            if self.using_async_client:
                crds = await self.custom_objects_api.list_namespaced_custom_object(
                    group="aiautoscaler.io",
                    version="v1",
                    namespace=self.namespace,
                    plural="aihorizontalpodautoscalers"
                )
            else:
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
                logger.warning(f"no crd config found in namespace {self.namespace}")
                return
            
            for crd in items:
                try:
                    await self._process_crd_configuration(crd)
                except Exception as crd_error:
                    crd_name = crd.get('metadata', {}).get('name', 'unknown')
                    logger.warning(f"failed to proccess crd : {crd_error} in {crd_name}")
                    continue
            
        except ApiException as e:
            if e.status == 404:
                error_msg = ("CRD 'aihorizontalpodautoscalers.aiautoscaler.io' not found in cluster")
                logger.error(f"{error_msg}")
            else:
                logger.error(f"k8s api error {e.status}: {e.reason}")
        except Exception as e:
            logger.error(f"unexpected error crd: {e}")

    async def _reload_crd_configurations(self):
        # Reload CRD configurations with error handling
        try:
            logger.info("reload crd config")
            await self._load_crd_configurations()
            logger.info(f"{len(self.monitored_deployments)} crd reload success")
        except Exception as e:
            logger.warning(f"failed crd reload : {e}")

    async def _process_crd_configuration(self, crd: dict):
        # Process a single CRD configuration with validation
        try:
            spec = crd.get('spec', {})
            metadata = crd.get('metadata', {})
            crd_name = metadata.get('name', 'unknown')
            
            deployment_name = spec.get('targetDeployment')
            if not deployment_name:
                logger.error("missing target deployment")
            
            validation_thresholds = spec.get('validationThresholds', {})
            thresholds = ValidationThresholds(
                max_historical_multiplier=validation_thresholds.get('maxHistoricalMultiplier', 2.0),
                max_spike_multiplier=validation_thresholds.get('maxSpikeMultiplier', 3.0)
            )

            prometheus_config = spec.get('prometheusConfig', {})
            if not spec.get('prometheusService'):
                logger.error("missing prometheus service")
            
            query_config = QueryConfig(
                service_name=spec.get('prometheusService'),
                window_minutes=prometheus_config.get('windowMinute', 10),
                query_template=prometheus_config.get('queryTemplate', 'rate(http_requests_total{{service="{service_name}"}}[{window}m])')
            )

            scaling_cfg = spec.get('scalingConfig', {})
            scaling_config = ScalingConfig(
                min_replicas=scaling_cfg.get('minReplicas', 1),
                max_replicas=scaling_cfg.get('maxReplicas', 10),
                workload_per_pod=scaling_cfg.get('workloadPerPod', 100),
                resource_removal_strategy=scaling_cfg.get('resourceRemovalStrategy', 'gradual'),
                cooldown_period=scaling_cfg.get('cooldownPeriod', 300)
            )

            logger.info(f"crd config loaded {crd_name} : {deployment_name}")
            
            self.monitored_deployments[deployment_name] = {
                'crd_name': crd_name,
                'thresholds': thresholds,
                'query_config': query_config,
                'scaling_config': scaling_config,
                'last_processed': None,
                'error_count': 0
            }
            
        except (ValueError, KeyError) as e:
            logger.error(f"invalid crd config : {e}")
        except Exception as e:
            logger.error(f"error crd config process : {e}")
    
    async def _get_current_replicas(self, deployment_name: str):
        # Get current replica count
        try:
            if self.using_async_client:
                deployment = await self.apps_v1.read_namespaced_deployment(
                    name=deployment_name,
                    namespace=self.namespace
                )
            else:
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
                logger.error(f"{deployment_name} not found in {self.namespace}")
            else:
                logger.error(f"error getting replicas for {deployment_name} (status {e.status}): {e.reason}")
            return None
        except Exception as e:
            logger.error(f"unexpected error getting replicas for {deployment_name}: {e}")
            return None
    
    async def _get_historical_metrics(self, query_config: QueryConfig):
        # Get historical metrics with error handling
        try:
            metrics = self.prometheus_client.get_historical_workload(query_config)
            
            if not metrics:
                logger.warning(f"no metrics returned from prometheus {query_config.service_name}")
                return None
            
            return metrics
            
        except Exception as e:
            logger.error(f" error getting historical metrics : {e}")
            return None
    
    async def _make_prediction(self, historical_data: list, thresholds: ValidationThresholds):
        # Make prediction with data validation
        try:
            if len(historical_data) != 10:
                if len(historical_data) < 10:
                    last_value = historical_data[-1] if historical_data else 0
                    historical_data.extend([last_value] * (10 - len(historical_data)))
                else:
                    historical_data = historical_data[-10:]

            scaler = MinMaxScaler(feature_range=(-1, 1))
            
            prediction = self.model_handler.predict(historical_data, scaler, thresholds)
            
            return prediction
            
        except Exception as e:
            logger.error(f"error making prediction : {e}")
            
            import traceback
            traceback.print_exc()
            return None
    
    async def _execute_scaling(self, deployment_name: str, scaling_decision):
        # Execute scaling action with error handling
        try:
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
                await asyncio.get_event_loop().run_in_executor(
                    None,
                    lambda: self.apps_v1.patch_namespaced_deployment(
                        name=deployment_name,
                        namespace=self.namespace,
                        body=body
                    )
                )
            
            logger.info(f"scaled {deployment_name} to {scaling_decision.target_replicas} replicas")
            return True
            
        except ApiException as e:
            if e.status == 404:
                logger.error(f"deployment {deployment_name} not found")
            elif e.status == 422:
                logger.error(f"invalid replica count : {e.reason}")
            else:
                logger.error(f"error api scaling deployment {deployment_name} with status {e.status} : {e.reason}")
            return False
        except Exception as e:
            logger.error(f"unexpected error scaling deployment {deployment_name} : {e}")
            return False

    async def _process_deployment(self, deployment_name: str, config: dict):
        # Process a single deployment
        try:
            print(f"\n{'='*60}")
            print(f"processing deployment: {deployment_name}")
            logger.info(f"processing deployment: {deployment_name}")
            print(f"{'='*60}")
            
            current_replicas = await self._get_current_replicas(deployment_name)
            if current_replicas is None:
                print(f"failed to get current {deployment_name} replicas")
                logger.error(f"failed to get current {deployment_name} replicas")
                config['error_count'] += 1
                return
            
            print(f" -> current replicas: {current_replicas}")
            
            historical_data = await self._get_historical_metrics(config['query_config'])
            
            if not historical_data:
                print(f"no historical data available for {deployment_name}")
                logger.error(f"no historical data available for {deployment_name}")

                self._log_to_csv(deployment_name, current_replicas)
                config['error_count'] += 1
                return
            
            current_request = historical_data[-1] if historical_data else None
            current_request_value = current_request[0] if current_request else None
            
            print(f" -> current request rate: {current_request_value}")
            
            predicted_workload = await self._make_prediction(historical_data, config['thresholds'])
            
            if predicted_workload is None:
                print(f"failed to make prediction for {deployment_name}")
                logger.error(f"failed to make prediction for {deployment_name}")

                self._log_to_csv(deployment_name, current_replicas, current_request_value)
                config['error_count'] += 1
                return
            
            print(f" -> predicted workload: {predicted_workload:.2f}")
            
            self._log_to_csv(deployment_name, current_replicas, current_request_value, predicted_workload)
            
            scaling_decision = self.scaling_algorithm.calculate_scaling_decision(
                deployment_name,
                predicted_workload,
                current_replicas,
                config['scaling_config']
            )
            
            print(f" -> scaling decision: {scaling_decision.action}")
            print(f" -> target replicas : {scaling_decision.target_replicas}")
            print(f" -> reason          : {scaling_decision.reason}")
            
            if scaling_decision.action in ["scale_out", "scale_in"]:
                print(f" -> executing scaling action: {scaling_decision.action}")
                logger.info(f"executing scaling action: {scaling_decision.action}")
                
                success = await self._execute_scaling(deployment_name, scaling_decision)
                if success:
                    self.scaling_algorithm.execute_scaling_decision(deployment_name, scaling_decision)
                    config['error_count'] = 0
                else:
                    config['error_count'] += 1
            else:
                print(" -> no scaling action needed")
                logger.info(" -> no scaling action needed")
                config['error_count'] = 0
            
            config['last_processed'] = datetime.now()
            
        except Exception as e:
            print(f"error processing deployment {deployment_name}: {e}")
            logger.error(f"error processing deployment {deployment_name}: {e}")
            
            import traceback
            traceback.print_exc()
            config['error_count'] += 1

    def _setup_signal_handlers(self):
        # Setup signal handlers for graceful shutdown
        def signal_handler(signum, frame):
            print(f"\n{'='*60}")
            print(f"received signal {signum}, initiating graceful shutdown")
            logger.info(f"received signal {signum}, initiating graceful shutdown")
            print(f"{'='*60}")
            
            self.running = False
            try:
                loop = asyncio.get_running_loop()
                loop.call_soon_threadsafe(self.shutdown_event.set)
            except RuntimeError:
                # No running loop, set directly
                self.shutdown_event.set()
        
        if hasattr(signal, 'SIGTERM'):
            signal.signal(signal.SIGTERM, signal_handler)
        if hasattr(signal, 'SIGINT'):
            signal.signal(signal.SIGINT, signal_handler)

    async def run(self):
        # Main operation loop
        self.running = True
        
        self._setup_signal_handlers()
        
        try:
            loop_count = 0
            while self.running and not self.shutdown_event.is_set():
                loop_count += 1
                loop_start_time = time.time()
                
                print(f"\n{'#'*60}")
                print(f"# loop {loop_count} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"# monitoring {len(self.monitored_deployments)} deployments")
                logger.info(f"monitoring {len(self.monitored_deployments)} deployments with loop {loop_count} - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"{'#'*60}")
                
                deployment_names = list(self.monitored_deployments.keys())
                
                if not deployment_names:
                    logger.warning("no deployment to monitor")
                
                for deployment_name in deployment_names:
                    if deployment_name not in self.monitored_deployments:
                        continue 
                        
                    config = self.monitored_deployments[deployment_name]
                    
                    try:
                        await self._process_deployment(deployment_name, config)
                    except Exception as e:
                        logger.error(f"critical error processing deployment {deployment_name}: {e}")
                        import traceback
                        traceback.print_exc()
                        config['error_count'] += 1
                    
                    if config['error_count'] > 10:
                        logger.warning(f"removing deployment {deployment_name} from monitoring")
                        logger.warning(f"reason: {config['error_count']} consecutive errors")
                        
                        del self.monitored_deployments[deployment_name]
                
                current_time = time.time()
                if current_time - self.last_crd_reload >= 600:
                    try:
                        await self._reload_crd_configurations()
                        self.last_crd_reload = current_time
                    except Exception as e:
                        logger.warning(f"failed to reload crd config : {e}")
                
                loop_duration = time.time() - loop_start_time
                sleep_time = max(0, 60 - loop_duration)
                
                print(f"\nloop completed in {loop_duration:.2f}s, sleeping for {sleep_time:.2f}s")
                logger.info(f"loop completed in {loop_duration:.2f}s, sleeping for {sleep_time:.2f}s")
                
                if sleep_time > 0:
                    try:
                        await asyncio.wait_for(self.shutdown_event.wait(), timeout=sleep_time)
                        break 
                    except asyncio.TimeoutError:
                        pass
                
        except Exception as e:
            logger.error(f"critical error in main loop : {e}")
            
            import traceback
            traceback.print_exc()
            raise

    async def shutdown(self):
        # Graceful shutdown with proper cleanup
        print("\n" + "="*60)
        print("shutting down AIHorizontalPodAutoscaler")
        logger.info("shutting down AIHorizontalPodAutoscaler")
        print("="*60)
        
        self.running = False
        self.shutdown_event.set()
        
        try:
            logger.info("write csv log file")
            self._write_csv_file()
        except Exception as e:
            logger.error(f"error writing csv file : {e}")
        
        # Close Kubernetes API clients properly
        try:
            if self.using_async_client:
                logger.info("close async k8s api")
                if self.apps_v1 and hasattr(self.apps_v1, 'api_client'):
                    await self.apps_v1.api_client.close()
                if self.custom_objects_api and hasattr(self.custom_objects_api, 'api_client'):
                    await self.custom_objects_api.api_client.close()
                logger.info("async close success")
        except Exception as e:
            logger.info(f"error closing api : {e}")
        
        print("Shutdown complete")
        logger.info("shutdown complete")

async def main():
    namespace = "test-autoscaler"
    operator = None
    
    print("="*60)
    print("AIHorizontalPodAutoscaler starting")
    print(f"namespace: {namespace}")
    print(f"time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"AIHorizontalPodAutoscaler starting for namespace : {namespace}")
    print("="*60 + "\n")
    
    # Create operator
    operator = AIHorizontalPodAutoscaler(namespace=namespace)
    
    try:
        logger.info("initializing operator")
        await operator.initialize()
        logger.info("operator initialized successfully")
        
        logger.info("starting main loop")
        
        await operator.run()
    except KeyboardInterrupt:
        logger.info("received keyboard interupt")
    except Exception as e:
        logger.error(f"operator failed : {e}")
        
        import traceback
        traceback.print_exc()
    finally:
        if operator:
            try:
                await operator.shutdown()
            except Exception as cleanup_error:
                logger.error(f"error during cleanup : {cleanup_error}")
                try:
                    operator._write_csv_file()
                except Exception as csv_error:
                    logger.error(f"error writing csv during cleanup : {csv_error}")

if __name__ == "__main__":
    exit_code = 0
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("received keyboard interrupt, shutting down")
        logger.info("received keyboard interupt, shutting down")
        
        exit_code = 0
    except Exception as e:
        logger.error(f"fatal error : {e}")
        
        import traceback
        traceback.print_exc()
        exit_code = 1
    finally:
        sys.exit(exit_code)