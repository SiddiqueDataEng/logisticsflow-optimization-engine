"""
LogisticsFlow Optimization Engine - Supply Chain Optimization DAG
Orchestrates route optimization, inventory management, and delivery prediction
"""

import os
import json
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Any

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.task_group import TaskGroup

# Import custom modules
import sys
sys.path.append('/opt/airflow/scripts')
from data_collectors.gps_tracking_collector import GPSTrackingCollector
from data_collectors.warehouse_collector import WarehouseCollector
from data_collectors.weather_collector import WeatherCollector
from data_collectors.traffic_collector import TrafficCollector
from optimization.route_optimizer import RouteOptimizer
from optimization.inventory_optimizer import InventoryOptimizer
from optimization.delivery_predictor import DeliveryPredictor
from utils.postgresql_client import PostgreSQLClient
from utils.redis_client import RedisClient

# Configuration
POSTGRES_HOST = Variable.get("POSTGRES_HOST", default_var="postgres")
REDIS_HOST = Variable.get("REDIS_HOST", default_var="redis")
OPTIMIZATION_REGIONS = Variable.get("OPTIMIZATION_REGIONS", default_var="north,south,east,west", deserialize_json=False).split(",")

# DAG Configuration
default_args = {
    'owner': 'logistics-optimization-team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
}

dag = DAG(
    'supply_chain_optimization',
    default_args=default_args,
    description='Supply chain and logistics optimization pipeline',
    schedule_interval=timedelta(minutes=30),  # Run every 30 minutes
    max_active_runs=1,
    tags=['logistics', 'optimization', 'supply-chain'],
)

def collect_vehicle_tracking_data(**context) -> Dict[str, Any]:
    """Collect real-time vehicle GPS tracking data"""
    collector = GPSTrackingCollector()
    
    try:
        # Get active vehicles
        active_vehicles = collector.get_active_vehicles()
        
        # Collect tracking data for each vehicle
        tracking_data = {}
        for vehicle in active_vehicles:
            vehicle_id = vehicle['vehicle_id']
            
            # Get current location and status
            current_location = collector.get_vehicle_location(vehicle_id)
            vehicle_status = collector.get_vehicle_status(vehicle_id)
            route_progress = collector.get_route_progress(vehicle_id)
            
            tracking_data[vehicle_id] = {
                'vehicle_id': vehicle_id,
                'current_location': current_location,
                'status': vehicle_status,
                'route_progress': route_progress,
                'last_updated': datetime.now().isoformat(),
                'driver_id': vehicle.get('driver_id'),
                'vehicle_type': vehicle.get('vehicle_type'),
                'capacity': vehicle.get('capacity')
            }
        
        logging.info(f"Collected tracking data for {len(tracking_data)} vehicles")
        return tracking_data
        
    except Exception as e:
        logging.error(f"Error collecting vehicle tracking data: {str(e)}")
        return {}

def collect_warehouse_data(**context) -> Dict[str, Any]:
    """Collect warehouse inventory and operations data"""
    collector = WarehouseCollector()
    
    try:
        warehouse_data = {}
        
        # Get all warehouses
        warehouses = collector.get_warehouses()
        
        for warehouse in warehouses:
            warehouse_id = warehouse['warehouse_id']
            
            # Get inventory levels
            inventory = collector.get_inventory_levels(warehouse_id)
            
            # Get pending orders
            pending_orders = collector.get_pending_orders(warehouse_id)
            
            # Get warehouse capacity and utilization
            capacity_info = collector.get_capacity_info(warehouse_id)
            
            # Get staff availability
            staff_info = collector.get_staff_availability(warehouse_id)
            
            warehouse_data[warehouse_id] = {
                'warehouse_id': warehouse_id,
                'location': warehouse.get('location'),
                'inventory': inventory,
                'pending_orders': pending_orders,
                'capacity_info': capacity_info,
                'staff_info': staff_info,
                'last_updated': datetime.now().isoformat()
            }
        
        logging.info(f"Collected data from {len(warehouse_data)} warehouses")
        return warehouse_data
        
    except Exception as e:
        logging.error(f"Error collecting warehouse data: {str(e)}")
        return {}

def collect_external_factors(**context) -> Dict[str, Any]:
    """Collect weather and traffic data affecting logistics"""
    weather_collector = WeatherCollector()
    traffic_collector = TrafficCollector()
    
    try:
        external_data = {
            'weather': {},
            'traffic': {},
            'collection_time': datetime.now().isoformat()
        }
        
        # Collect weather data for each region
        for region in OPTIMIZATION_REGIONS:
            try:
                # Get region coordinates (simplified - in practice, use proper region mapping)
                region_coords = {
                    'north': {'lat': 40.7589, 'lon': -73.9851},
                    'south': {'lat': 40.6892, 'lon': -74.0445},
                    'east': {'lat': 40.7282, 'lon': -73.7949},
                    'west': {'lat': 40.7589, 'lon': -74.0060}
                }.get(region, {'lat': 40.7589, 'lon': -73.9851})
                
                # Get weather conditions
                weather_data = weather_collector.get_current_weather(
                    region_coords['lat'], 
                    region_coords['lon']
                )
                
                # Get traffic conditions
                traffic_data = traffic_collector.get_traffic_conditions(
                    region_coords['lat'], 
                    region_coords['lon'],
                    radius_km=10
                )
                
                external_data['weather'][region] = weather_data
                external_data['traffic'][region] = traffic_data
                
            except Exception as e:
                logging.warning(f"Error collecting external data for region {region}: {str(e)}")
                continue
        
        logging.info(f"Collected external factors for {len(OPTIMIZATION_REGIONS)} regions")
        return external_data
        
    except Exception as e:
        logging.error(f"Error collecting external factors: {str(e)}")
        return {}

def optimize_delivery_routes(**context) -> Dict[str, Any]:
    """Optimize delivery routes using advanced algorithms"""
    optimizer = RouteOptimizer()
    
    # Pull data from previous tasks
    vehicle_data = context['ti'].xcom_pull(task_ids='collect_vehicle_tracking_data')
    warehouse_data = context['ti'].xcom_pull(task_ids='collect_warehouse_data')
    external_data = context['ti'].xcom_pull(task_ids='collect_external_factors')
    
    try:
        optimization_results = {}
        
        for region in OPTIMIZATION_REGIONS:
            try:
                # Get pending deliveries for region
                pending_deliveries = []
                for warehouse_id, wh_data in warehouse_data.items():
                    for order in wh_data.get('pending_orders', []):
                        if order.get('delivery_region') == region:
                            pending_deliveries.append(order)
                
                if not pending_deliveries:
                    continue
                
                # Get available vehicles in region
                available_vehicles = []
                for vehicle_id, vehicle_info in vehicle_data.items():
                    if (vehicle_info.get('status') == 'available' and 
                        vehicle_info.get('region') == region):
                        available_vehicles.append(vehicle_info)
                
                # Get external factors for region
                weather_conditions = external_data.get('weather', {}).get(region, {})
                traffic_conditions = external_data.get('traffic', {}).get(region, {})
                
                # Optimize routes
                optimization_input = {
                    'deliveries': pending_deliveries,
                    'vehicles': available_vehicles,
                    'weather': weather_conditions,
                    'traffic': traffic_conditions,
                    'optimization_objectives': ['minimize_distance', 'minimize_time', 'maximize_efficiency']
                }
                
                route_optimization = optimizer.optimize_routes(optimization_input)
                
                optimization_results[region] = {
                    'region': region,
                    'optimized_routes': route_optimization.get('routes', []),
                    'total_distance': route_optimization.get('total_distance', 0),
                    'total_time': route_optimization.get('total_time', 0),
                    'cost_savings': route_optimization.get('cost_savings', 0),
                    'efficiency_score': route_optimization.get('efficiency_score', 0),
                    'optimization_time': datetime.now().isoformat()
                }
                
                logging.info(f"Optimized routes for region {region}: {len(route_optimization.get('routes', []))} routes")
                
            except Exception as e:
                logging.error(f"Error optimizing routes for region {region}: {str(e)}")
                continue
        
        return optimization_results
        
    except Exception as e:
        logging.error(f"Error in route optimization: {str(e)}")
        return {}

def optimize_inventory_levels(**context) -> Dict[str, Any]:
    """Optimize inventory levels across warehouses"""
    optimizer = InventoryOptimizer()
    
    # Pull warehouse data
    warehouse_data = context['ti'].xcom_pull(task_ids='collect_warehouse_data')
    
    try:
        inventory_optimization = {}
        
        for warehouse_id, wh_data in warehouse_data.items():
            try:
                # Get current inventory
                current_inventory = wh_data.get('inventory', {})
                
                # Get historical demand (simplified - would come from database)
                historical_demand = optimizer.get_historical_demand(warehouse_id, days=30)
                
                # Get supplier information
                supplier_info = optimizer.get_supplier_info(warehouse_id)
                
                # Optimize inventory levels
                optimization_input = {
                    'current_inventory': current_inventory,
                    'historical_demand': historical_demand,
                    'supplier_info': supplier_info,
                    'capacity_constraints': wh_data.get('capacity_info', {}),
                    'cost_parameters': {
                        'holding_cost_rate': 0.25,  # 25% annual holding cost
                        'stockout_cost_multiplier': 5.0,
                        'ordering_cost': 100.0
                    }
                }
                
                inventory_result = optimizer.optimize_inventory(optimization_input)
                
                inventory_optimization[warehouse_id] = {
                    'warehouse_id': warehouse_id,
                    'recommended_orders': inventory_result.get('recommended_orders', []),
                    'optimal_stock_levels': inventory_result.get('optimal_stock_levels', {}),
                    'reorder_points': inventory_result.get('reorder_points', {}),
                    'safety_stock_levels': inventory_result.get('safety_stock_levels', {}),
                    'cost_reduction': inventory_result.get('cost_reduction', 0),
                    'service_level': inventory_result.get('service_level', 0),
                    'optimization_time': datetime.now().isoformat()
                }
                
                logging.info(f"Optimized inventory for warehouse {warehouse_id}")
                
            except Exception as e:
                logging.error(f"Error optimizing inventory for warehouse {warehouse_id}: {str(e)}")
                continue
        
        return inventory_optimization
        
    except Exception as e:
        logging.error(f"Error in inventory optimization: {str(e)}")
        return {}

def predict_delivery_times(**context) -> Dict[str, Any]:
    """Predict delivery times using ML models"""
    predictor = DeliveryPredictor()
    
    # Pull data from previous tasks
    route_optimization = context['ti'].xcom_pull(task_ids='optimize_delivery_routes')
    external_data = context['ti'].xcom_pull(task_ids='collect_external_factors')
    
    try:
        delivery_predictions = {}
        
        for region, route_data in route_optimization.items():
            try:
                optimized_routes = route_data.get('optimized_routes', [])
                
                for route in optimized_routes:
                    route_id = route.get('route_id')
                    
                    # Prepare prediction input
                    prediction_input = {
                        'route': route,
                        'weather_conditions': external_data.get('weather', {}).get(region, {}),
                        'traffic_conditions': external_data.get('traffic', {}).get(region, {}),
                        'historical_performance': predictor.get_historical_performance(route_id),
                        'vehicle_characteristics': route.get('vehicle_info', {}),
                        'driver_performance': predictor.get_driver_performance(route.get('driver_id'))
                    }
                    
                    # Generate predictions
                    prediction_result = predictor.predict_delivery_times(prediction_input)
                    
                    delivery_predictions[route_id] = {
                        'route_id': route_id,
                        'region': region,
                        'predicted_delivery_times': prediction_result.get('delivery_times', []),
                        'confidence_intervals': prediction_result.get('confidence_intervals', []),
                        'risk_factors': prediction_result.get('risk_factors', []),
                        'recommended_buffer_time': prediction_result.get('buffer_time', 0),
                        'prediction_accuracy': prediction_result.get('accuracy_score', 0),
                        'prediction_time': datetime.now().isoformat()
                    }
                
                logging.info(f"Generated delivery predictions for {len(optimized_routes)} routes in {region}")
                
            except Exception as e:
                logging.error(f"Error predicting delivery times for region {region}: {str(e)}")
                continue
        
        return delivery_predictions
        
    except Exception as e:
        logging.error(f"Error in delivery time prediction: {str(e)}")
        return {}

def store_optimization_results(**context) -> None:
    """Store optimization results to PostgreSQL"""
    db_client = PostgreSQLClient(POSTGRES_HOST)
    
    # Pull results from previous tasks
    route_optimization = context['ti'].xcom_pull(task_ids='optimize_delivery_routes')
    inventory_optimization = context['ti'].xcom_pull(task_ids='optimize_inventory_levels')
    delivery_predictions = context['ti'].xcom_pull(task_ids='predict_delivery_times')
    
    try:
        # Store route optimization results
        if route_optimization:
            db_client.store_route_optimization_results(route_optimization)
            logging.info("Stored route optimization results")
        
        # Store inventory optimization results
        if inventory_optimization:
            db_client.store_inventory_optimization_results(inventory_optimization)
            logging.info("Stored inventory optimization results")
        
        # Store delivery predictions
        if delivery_predictions:
            db_client.store_delivery_predictions(delivery_predictions)
            logging.info("Stored delivery predictions")
            
    except Exception as e:
        logging.error(f"Error storing optimization results: {str(e)}")
        raise

def update_real_time_cache(**context) -> None:
    """Update Redis cache with optimization results"""
    redis_client = RedisClient(REDIS_HOST)
    
    # Pull results from previous tasks
    route_optimization = context['ti'].xcom_pull(task_ids='optimize_delivery_routes')
    inventory_optimization = context['ti'].xcom_pull(task_ids='optimize_inventory_levels')
    delivery_predictions = context['ti'].xcom_pull(task_ids='predict_delivery_times')
    
    try:
        # Cache route optimization results
        if route_optimization:
            for region, data in route_optimization.items():
                redis_client.setex(f"routes:optimized:{region}", 1800, json.dumps(data))  # 30 min TTL
        
        # Cache inventory recommendations
        if inventory_optimization:
            for warehouse_id, data in inventory_optimization.items():
                redis_client.setex(f"inventory:optimized:{warehouse_id}", 3600, json.dumps(data))  # 1 hour TTL
        
        # Cache delivery predictions
        if delivery_predictions:
            for route_id, data in delivery_predictions.items():
                redis_client.setex(f"delivery:prediction:{route_id}", 1800, json.dumps(data))  # 30 min TTL
        
        # Cache summary metrics
        summary_metrics = {
            'total_optimized_routes': sum(len(data.get('optimized_routes', [])) for data in route_optimization.values()) if route_optimization else 0,
            'total_warehouses_optimized': len(inventory_optimization) if inventory_optimization else 0,
            'total_delivery_predictions': len(delivery_predictions) if delivery_predictions else 0,
            'last_optimization_time': datetime.now().isoformat()
        }
        
        redis_client.setex("logistics:summary", 3600, json.dumps(summary_metrics))
        
        logging.info("Updated Redis cache with optimization results")
        
    except Exception as e:
        logging.error(f"Error updating Redis cache: {str(e)}")
        raise

def generate_optimization_alerts(**context) -> None:
    """Generate alerts for critical optimization insights"""
    # Pull results from previous tasks
    route_optimization = context['ti'].xcom_pull(task_ids='optimize_delivery_routes')
    inventory_optimization = context['ti'].xcom_pull(task_ids='optimize_inventory_levels')
    delivery_predictions = context['ti'].xcom_pull(task_ids='predict_delivery_times')
    
    try:
        alerts = []
        
        # Check for route optimization alerts
        if route_optimization:
            for region, data in route_optimization.items():
                efficiency_score = data.get('efficiency_score', 0)
                if efficiency_score < 0.7:  # Below 70% efficiency
                    alerts.append({
                        'type': 'route_efficiency',
                        'severity': 'medium',
                        'region': region,
                        'message': f"Route efficiency in {region} is below threshold: {efficiency_score:.2f}",
                        'recommendation': 'Review route optimization parameters and constraints'
                    })
        
        # Check for inventory alerts
        if inventory_optimization:
            for warehouse_id, data in inventory_optimization.items():
                service_level = data.get('service_level', 1.0)
                if service_level < 0.95:  # Below 95% service level
                    alerts.append({
                        'type': 'inventory_service_level',
                        'severity': 'high',
                        'warehouse_id': warehouse_id,
                        'message': f"Service level in warehouse {warehouse_id} is below target: {service_level:.2f}",
                        'recommendation': 'Increase safety stock levels or review demand forecasting'
                    })
        
        # Check for delivery prediction alerts
        if delivery_predictions:
            for route_id, data in delivery_predictions.items():
                risk_factors = data.get('risk_factors', [])
                if len(risk_factors) > 2:  # Multiple risk factors
                    alerts.append({
                        'type': 'delivery_risk',
                        'severity': 'medium',
                        'route_id': route_id,
                        'message': f"Route {route_id} has multiple risk factors: {', '.join(risk_factors)}",
                        'recommendation': 'Consider alternative routes or adjust delivery schedule'
                    })
        
        # Store alerts (simplified - in practice, send to notification system)
        if alerts:
            logging.warning(f"Generated {len(alerts)} optimization alerts")
            for alert in alerts:
                logging.warning(f"ALERT: {alert['message']}")
        else:
            logging.info("No optimization alerts generated")
            
    except Exception as e:
        logging.error(f"Error generating optimization alerts: {str(e)}")

# Task definitions
with TaskGroup("data_collection", dag=dag) as data_collection_group:
    
    collect_vehicles = PythonOperator(
        task_id='collect_vehicle_tracking_data',
        python_callable=collect_vehicle_tracking_data,
    )
    
    collect_warehouses = PythonOperator(
        task_id='collect_warehouse_data',
        python_callable=collect_warehouse_data,
    )
    
    collect_external = PythonOperator(
        task_id='collect_external_factors',
        python_callable=collect_external_factors,
    )

with TaskGroup("optimization", dag=dag) as optimization_group:
    
    optimize_routes = PythonOperator(
        task_id='optimize_delivery_routes',
        python_callable=optimize_delivery_routes,
    )
    
    optimize_inventory = PythonOperator(
        task_id='optimize_inventory_levels',
        python_callable=optimize_inventory_levels,
    )
    
    predict_deliveries = PythonOperator(
        task_id='predict_delivery_times',
        python_callable=predict_delivery_times,
    )

store_results = PythonOperator(
    task_id='store_optimization_results',
    python_callable=store_optimization_results,
    dag=dag,
)

update_cache = PythonOperator(
    task_id='update_real_time_cache',
    python_callable=update_real_time_cache,
    dag=dag,
)

generate_alerts = PythonOperator(
    task_id='generate_optimization_alerts',
    python_callable=generate_optimization_alerts,
    dag=dag,
)

# Task dependencies
data_collection_group >> optimization_group >> store_results >> [update_cache, generate_alerts]