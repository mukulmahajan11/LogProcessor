import json
import os
import time
import uuid
from confluent_kafka import SerializingProducer
import simplejson 
from datetime import datetime, timedelta
import random
from datasketch import MinHash, MinHashLSH
from pybloom_live import BloomFilter
import time
import numpy as np



LOS_ANGELES_COORDINATES = { "latitude": 34.0522, "longitude": -118.2437 }
SAN_FRANCISCO_COORDINATES = {"latitude": 37.7749, "longitude": -122.4194}
#movement increments
LATITUDE_INCREMENT = (SAN_FRANCISCO_COORDINATES['latitude'] - LOS_ANGELES_COORDINATES ['latitude' ]) / 100
LONGITUDE_INCREMENT = (SAN_FRANCISCO_COORDINATES['longitude']- LOS_ANGELES_COORDINATES ['longitude']) / 100

#Environment Variables for configuration
KAFKA_BOOTSTRAP_SERVERS = os.getenv( 'KAFKA_BOOTSTRAP_SERVERS', 'Localhost: 9093')
VEHICLE_TOPIC = os.getenv( 'VEHICLE_TOPIC', 'vehicle_data')
GPS_TOPIC = os.getenv( 'GPS_TOPIC', 'gps_data')
TRAFFIC_TOPIC = os.getenv( 'TRAFFIC_TOPIC', 'traffic_data')
WEATHER_TOPIC = os.getenv( 'WEATHER_TOPIC', 'weather_data')
EMERGENCY_TOPIC = os.getenv( 'EMERGENCY_TOPIC', 'emergency_data')

start_time = datetime.now()
start_location = LOS_ANGELES_COORDINATES.copy()
weather_lsh = MinHashLSH(threshold=0.1, num_perm=128)


def apply_differential_privacy(data, sensitivity, epsilon):
    """Apply Laplace noise to data for differential privacy.

    Args:
        data (float): The original data point to be privatized.
        sensitivity (float): The maximum change to the query function from a single alteration of the dataset.
        epsilon (float): Privacy budget, lower values are more private.

    Returns:
        float: Privatized data.
    """
    scale = sensitivity / epsilon
    noise = np.random.laplace(0, scale, 1)
    return data + noise


def report_average_speed(speeds, epsilon=1.0):
    """Calculate differentially private average speed.

    Args:
        speeds (list of float): List of speed measurements.
        epsilon (float): Privacy budget, with default set for moderate privacy.

    Returns:
        float: Differentially private average speed.
    """
    average_speed = sum(speeds) / len(speeds)
    # Sensitivity of average speed is the maximum speed divided by the number of data points (assuming speed is bounded)
    sensitivity = max(speeds) / len(speeds)
    private_average = apply_differential_privacy(average_speed, sensitivity, epsilon)
    return private_average


def get_next_time():
    global start_time
    start_time += timedelta(seconds=random.randint(30,60))

    return start_time

def create_minhash(data, num_perm=128):
    m = MinHash(num_perm=num_perm)
    for key, value in data.items():
        m.update(f"{key}_{value}".encode('utf-8'))
    return m

def add_data_to_lsh(data, lsh):
    minhash = create_minhash(data)
    data_id = str(uuid.uuid4())
    lsh.insert(data_id, minhash)
    similar_items = lsh.query(minhash)
    if len(similar_items) > 1:
        print(f"Found similar entries for ID {data_id}: {similar_items}")
    return data


def generate_weather_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'location': location,
        'timestamp': timestamp,
        'temperature': random.uniform(-5, 26),
        'weatherCondition': random.choice(['Sunny', 'Cloudy', 'Rain', 'Snow']),
        'precipitation': random.uniform(0, 25),
        'windSpeed': random.uniform(0, 100),
        'humidity': random.randint(0, 100), # percentage
        'airQualityIndex': random.uniform(0, 500) # AQL Value goes here
        }

def generate_emergency_incident_data(device_id, timestamp, location):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'incidentId': uuid.uuid4(),
        'type': random.choice(['Accident', 'Fire', 'Medical', 'Police', 'None']),
        'timestamp': timestamp,
        'location': location,
        'status': random.choice(['Active', 'Resolved']),
        'description': 'Description of the incident'
        }


def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': timestamp,
        'speed': random.uniform(0, 40), 
        'direction': 'North-East',
        'vehicleType': vehicle_type
        }

def generate_traffic_camera_data(device_id, timestamp, location, camera_id):
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'cameraId': camera_id,
        'location': location,
        'timestamp': timestamp,
        'snapshot': 'Base64EncodedString'
        }


def simulate_vehicle_movement():
    global start_location
#move towards San Francisco
    start_location[ 'latitude'] += LATITUDE_INCREMENT
    start_location['longitude'] += LONGITUDE_INCREMENT
#add some randomness to pimulate actual road travel 
    start_location[ 'latitude'] += random.uniform(-0.0005,0.0005)
    start_location[ 'longitude'] += random.uniform(-0.0005,0.0005)

    return start_location


def generate_vehicle_data(device_id):   
    location = simulate_vehicle_movement()
    return {
        'id': uuid.uuid4(),
        'deviceId': device_id,
        'timestamp': get_next_time().isoformat(),
        'location': (location['latitude'], location['longitude']),
        'speed': random.uniform(10,40),
        'direction': 'North-East',
        'make': 'BMW',
        'model': 'C500',
        'year': 2024,
        'fuelType': 'Hybrid'

}

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')
    else:
        print(f'Message delivered to {msg.topic()} [{msg.partition()}]')
def json_serializer(o):
    if isinstance(o, uuid.UUID):
        return str(o)
    raise TypeError(f'Object of type{o._class.name_} is not JSON serializable')



def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serializer).encode('utf-8'),
        on_delivery=delivery_report
    )
    producer.flush()


def simulate_journey(producer, device_id):
    # Initialize the Bloom filter with desired capacity and error rate
    vehicle_ids_bloom = BloomFilter(capacity=1000, error_rate=0.01)

    while True:
        vehicle_data = generate_vehicle_data(device_id)

        # Check if the vehicle ID has already been seen
        if vehicle_data['deviceId'] in vehicle_ids_bloom:
            print(f"Duplicate vehicle data detected: {vehicle_data['deviceId']}")
        else:
            # If not seen, add to Bloom filter and process further
            vehicle_ids_bloom.add(vehicle_data['deviceId'])

            # Generate additional data types
            gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
            traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'],
                                                               vehicle_data['location'], 'Niconcamera')
            weather_data = generate_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
            emergency_incident_data = generate_emergency_incident_data(device_id, vehicle_data['timestamp'],
                                                                       vehicle_data['location'])

            # Produce data to Kafka topics
            produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
            produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
            produce_data_to_kafka(producer, TRAFFIC_TOPIC, traffic_camera_data)
            produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
            produce_data_to_kafka(producer, EMERGENCY_TOPIC, emergency_incident_data)

        # Check if the vehicle has reached its destination
        if (vehicle_data['location'][0] >= SAN_FRANCISCO_COORDINATES['latitude'] and
                vehicle_data['location'][1] <= SAN_FRANCISCO_COORDINATES['longitude']):
            print(f'Vehicle has reached San Francisco. Simulation has ended.')
            break

        # Pause between iterations to simulate real-time data streaming
        time.sleep(5)
    speeds_collected = [30, 35, 40, 25, 30]  # Example speeds collected during a simulation step
    epsilon_value = 0.5  # Smaller epsilon means more privacy
    private_average_speed = report_average_speed(speeds_collected, epsilon=epsilon_value)
    print(f"Reported (Differentially Private) Average Speed: {private_average_speed}")


if _name_ == '_main_':
    producer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'error_cb': lambda err: print(f'Kafka error: {err}')
    }
    producer = SerializingProducer(producer_config)

    try:
        simulate_journey(producer,'Vehicle-Vaibhav-123')
    except KeyboardInterrupt:
        print( 'Simulation ended by the user')
    except Exception as e:
        print(f' Unexpected Error occurred:{e}')
