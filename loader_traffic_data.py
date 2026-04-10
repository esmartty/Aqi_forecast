import datetime
import logging
from sqlalchemy import text
from traffic import json_data, parse_traffic_metrics

logger = logging.getLogger(__name__)


def query_pillar_points(connection):
    """Fetch all pillar_points from the database."""
    sql = text("""
        SELECT id, latitude, longitude 
        FROM public.pillar_points
        ORDER BY id
    """)
    
    try:
        result = connection.execute(sql)
        pillar_points = [
            {
                "id": row[0],
                "latitude": row[1],
                "longitude": row[2]
            }
            for row in result
        ]
        logger.info(f"Fetched {len(pillar_points)} pillar_points from database")
        return pillar_points
    except Exception as e:
        logger.exception(f"Error querying pillar_points: {e}")
        raise


def fetch_traffic_for_point(lat, lon):
    """
    Fetch traffic data from TomTom API for a given latitude/longitude.
    
    Returns dict with parsed traffic metrics or None on error.
    """
    try:
        response = json_data(lat, lon)
        if response is None:
            return None
        
        metrics = parse_traffic_metrics(response)
        return metrics
    except Exception as e:
        logger.warning(f"Error fetching traffic data for point ({lat}, {lon}): {e}")
        return None


def insert_traffic_data(connection):
    """
    Fetch current traffic data for all pillar_points and insert as historical record.
    
    Inserts one record per pillar_point with current timestamp and measurement_hour.
    Uses savepoint-per-row approach to continue on individual insert failures.
    """
    # Fetch all pillar_points
    try:
        pillar_points = query_pillar_points(connection)
    except Exception as e:
        logger.exception(f"Failed to fetch pillar_points: {e}")
        return
    
    if not pillar_points:
        logger.warning("No pillar_points found in database")
        return
    
    # Get current date and hour for all records
    now = datetime.datetime.now()
    measure_date = now.date()
    measurement_hour = now.hour
    date_update = now
    
    # SQL for inserting traffic data
    insert_sql = text("""
        INSERT INTO public.traffic_data (
            pillar_point_id, date, measurement_hour, 
            current_speed, free_flow_speed, current_travel_time, 
            free_flow_travel_time, road_closure, functional_road_class, 
            confidence, coordinates, date_update
        )
        VALUES (
            :pillar_point_id, :date, :measurement_hour,
            :current_speed, :free_flow_speed, :current_travel_time,
            :free_flow_travel_time, :road_closure, :functional_road_class,
            :confidence, :coordinates, :date_update
        )
        ON CONFLICT (pillar_point_id, date, measurement_hour)
        DO UPDATE SET
            current_speed = EXCLUDED.current_speed,
            free_flow_speed = EXCLUDED.free_flow_speed,
            current_travel_time = EXCLUDED.current_travel_time,
            free_flow_travel_time = EXCLUDED.free_flow_travel_time,
            road_closure = EXCLUDED.road_closure,
            functional_road_class = EXCLUDED.functional_road_class,
            confidence = EXCLUDED.confidence,
            coordinates = EXCLUDED.coordinates,
            date_update = EXCLUDED.date_update
    """)
    
    success_count = 0
    failure_count = 0
    
    # Process each pillar_point
    for pillar in pillar_points:
        pillar_id = pillar["id"]
        lat = pillar["latitude"]
        lon = pillar["longitude"]
        
        try:
            # Fetch traffic data from API
            metrics = fetch_traffic_for_point(lat, lon)
            
            if metrics is None:
                logger.warning(f"No traffic data fetched for pillar_point {pillar_id} ({lat}, {lon})")
                failure_count += 1
                continue
            
            # Insert record with savepoint
            try:
                with connection.begin_nested():
                    connection.execute(insert_sql, {
                        "pillar_point_id": pillar_id,
                        "date": measure_date,
                        "measurement_hour": measurement_hour,
                        "current_speed": metrics.get("current_speed"),
                        "free_flow_speed": metrics.get("free_flow_speed"),
                        "current_travel_time": metrics.get("current_travel_time"),
                        "free_flow_travel_time": metrics.get("free_flow_travel_time"),
                        "road_closure": metrics.get("road_closure"),
                        "functional_road_class": metrics.get("functional_road_class"),
                        "confidence": metrics.get("confidence"),
                        "coordinates": metrics.get("coordinates"),
                        "date_update": date_update,
                    })
                    success_count += 1
                    logger.debug(f"Inserted traffic data for pillar_point {pillar_id}")
            except Exception as e:
                logger.exception(f"Error inserting traffic data for pillar_point {pillar_id}: {e}")
                failure_count += 1
        
        except Exception as e:
            logger.exception(f"Unexpected error processing pillar_point {pillar_id}: {e}")
            failure_count += 1
    
    # Log summary
    logger.info(f"Traffic data insertion complete: {success_count} succeeded, {failure_count} failed")
