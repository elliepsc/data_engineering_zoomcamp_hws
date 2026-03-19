"""
BigQuery Schema Definitions for NYC Taxi Data

Explicit schemas prevent autodetect issues where field types change between months.
For example: ehail_fee changes from FLOAT (Jan-Aug 2019) to INTEGER (Sep+ 2019).

By defining explicit schemas, we ensure consistent types across all data loads.
"""

# Green Taxi Schema
# Based on TLC data dictionary: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page
GREEN_TAXI_SCHEMA = [
    # Trip identifiers
    {'name': 'VendorID', 'type': 'INTEGER', 'mode': 'NULLABLE', 
     'description': '1=Creative Mobile Technologies, 2=VeriFone Inc'},
    
    # Timestamps
    {'name': 'lpep_pickup_datetime', 'type': 'TIMESTAMP', 'mode': 'NULLABLE',
     'description': 'Date and time when the meter was engaged'},
    {'name': 'lpep_dropoff_datetime', 'type': 'TIMESTAMP', 'mode': 'NULLABLE',
     'description': 'Date and time when the meter was disengaged'},
    
    # Passenger and trip details
    {'name': 'store_and_fwd_flag', 'type': 'STRING', 'mode': 'NULLABLE',
     'description': 'Y=store and forward trip, N=not a store and forward trip'},
    {'name': 'RatecodeID', 'type': 'INTEGER', 'mode': 'NULLABLE',
     'description': 'Final rate code: 1-6'},
    {'name': 'PULocationID', 'type': 'INTEGER', 'mode': 'NULLABLE',
     'description': 'TLC Taxi Zone in which the meter was engaged'},
    {'name': 'DOLocationID', 'type': 'INTEGER', 'mode': 'NULLABLE',
     'description': 'TLC Taxi Zone in which the meter was disengaged'},
    
    # Passenger count - FLOAT to handle inconsistent data
    {'name': 'passenger_count', 'type': 'FLOAT', 'mode': 'NULLABLE',
     'description': 'Number of passengers (sometimes decimal in source data)'},
    
    # Distance and duration
    {'name': 'trip_distance', 'type': 'FLOAT', 'mode': 'NULLABLE',
     'description': 'Trip distance in miles'},
    
    # Fare components - ALL FLOAT for consistency
    {'name': 'fare_amount', 'type': 'FLOAT', 'mode': 'NULLABLE',
     'description': 'Time-and-distance fare calculated by meter'},
    {'name': 'extra', 'type': 'FLOAT', 'mode': 'NULLABLE',
     'description': 'Miscellaneous extras and surcharges'},
    {'name': 'mta_tax', 'type': 'FLOAT', 'mode': 'NULLABLE',
     'description': 'MTA tax automatically triggered'},
    {'name': 'tip_amount', 'type': 'FLOAT', 'mode': 'NULLABLE',
     'description': 'Tip amount (credit card only)'},
    {'name': 'tolls_amount', 'type': 'FLOAT', 'mode': 'NULLABLE',
     'description': 'Total amount of all tolls paid'},
    
    # CRITICAL: ehail_fee is FLOAT not INTEGER
    # This field changes type between months in source data
    {'name': 'ehail_fee', 'type': 'FLOAT', 'mode': 'NULLABLE',
     'description': 'E-hail fee'},
    
    {'name': 'improvement_surcharge', 'type': 'FLOAT', 'mode': 'NULLABLE',
     'description': 'Improvement surcharge assessed'},
    {'name': 'total_amount', 'type': 'FLOAT', 'mode': 'NULLABLE',
     'description': 'Total amount charged to passengers'},
    {'name': 'congestion_surcharge', 'type': 'FLOAT', 'mode': 'NULLABLE',
     'description': 'Congestion surcharge (added 2019)'},
    
    # Payment details
    {'name': 'payment_type', 'type': 'INTEGER', 'mode': 'NULLABLE',
     'description': '1=Credit card, 2=Cash, 3=No charge, 4=Dispute, 5=Unknown, 6=Voided'},
    {'name': 'trip_type', 'type': 'INTEGER', 'mode': 'NULLABLE',
     'description': '1=Street-hail, 2=Dispatch'},
]

# Yellow Taxi Schema (for future use)
YELLOW_TAXI_SCHEMA = [
    {'name': 'VendorID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'tpep_pickup_datetime', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
    {'name': 'tpep_dropoff_datetime', 'type': 'TIMESTAMP', 'mode': 'NULLABLE'},
    {'name': 'passenger_count', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'trip_distance', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'RatecodeID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'store_and_fwd_flag', 'type': 'STRING', 'mode': 'NULLABLE'},
    {'name': 'PULocationID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'DOLocationID', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'payment_type', 'type': 'INTEGER', 'mode': 'NULLABLE'},
    {'name': 'fare_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'extra', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'mta_tax', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'tip_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'tolls_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'improvement_surcharge', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'total_amount', 'type': 'FLOAT', 'mode': 'NULLABLE'},
    {'name': 'congestion_surcharge', 'type': 'FLOAT', 'mode': 'NULLABLE'},
]


def get_schema(taxi_type: str) -> list:
    """
    Get BigQuery schema for specified taxi type.
    
    Args:
        taxi_type: 'green' or 'yellow'
        
    Returns:
        List of schema field dictionaries
        
    Raises:
        ValueError: If taxi_type is not supported
    """
    schemas = {
        'green': GREEN_TAXI_SCHEMA,
        'yellow': YELLOW_TAXI_SCHEMA,
    }
    
    if taxi_type not in schemas:
        raise ValueError(f"Unsupported taxi type: {taxi_type}. Use 'green' or 'yellow'.")
    
    return schemas[taxi_type]
