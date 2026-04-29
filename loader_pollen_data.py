import logging
from sqlalchemy import create_engine, text
import pollen
from parser import parse_numeric

logger = logging.getLogger(__name__)

def query_coord_to_id(connection):
    """Fetch all camps_points from the database."""
    sql = text("""
        SELECT id, latitude, longitude 
        FROM public.camps_points
        ORDER BY id
    """)
    
    try:
        result = connection.execute(sql)
        coord_to_id = {(round(row[1], 5), round(row[2], 5)): row[0] for row in result}
        logger.info(f"Fetched {len(coord_to_id)} camps_points from database")
        return coord_to_id
    except Exception as e:
        logger.exception(f"Error querying camps_points: {e}")
        raise


def insert_camps_point(connection, latitude, longitude):
    """Insert camp point and return existing one (by coords)."""

    latitude = round(latitude, 5)
    longitude = round(longitude, 5)

    sql = text("""
        INSERT INTO public.camps_points (latitude, longitude, geom)
        VALUES (:lat, :lng, ST_SetSRID(ST_MakePoint(:lng, :lat), 4326))
        ON CONFLICT (latitude, longitude)
        DO UPDATE SET latitude = EXCLUDED.latitude,
                     longitude = EXCLUDED.longitude,
                     geom = EXCLUDED.geom
        RETURNING id
    """)

    params = {
        "lat": latitude,
        "lng": longitude
    }

    result = connection.execute(sql, params)
    return result.scalar()


def get_id_by_coord(connection, latitude, longitude, coord_to_id=None):
    """Get the ID of a camp point by its coordinates."""
    latitude = round(latitude, 5)
    longitude = round(longitude, 5)

    key = (latitude, longitude)

    camps_point_id = coord_to_id.get(key)

    if camps_point_id is not None:
        return camps_point_id
    
    else:
    
        new_id = insert_camps_point(connection, latitude, longitude)

        coord_to_id[key] = new_id
            
        return new_id


def insert_pollen_data(connection, start_date, end_date):
    """Insert pollen data for date range."""
    from datetime import datetime, timedelta

    current_date = datetime.strptime(start_date, "%Y-%m-%d").date()
    end = datetime.strptime(end_date, "%Y-%m-%d").date()

    while current_date <= end:
        date_str = current_date.strftime("%Y-%m-%d")
        try:
            df = pollen.run_pipeline(date_str)
        except Exception as e:
            logger.exception(f"Error loading pollen data for {date_str}: {e}")
            current_date += timedelta(days=1)
            continue

        sql = text("""INSERT INTO public.pollen_data(camps_point_id, forecast_time, analysis_time, alder_pollen, birch_pollen, grass_pollen, mugwort_pollen, olive_pollen, ragweed_pollen, pollen_total, date_update)
                VALUES (:camps_point_id, :forecast_time, :analysis_time, :alder_pollen, :birch_pollen, :grass_pollen, :mugwort_pollen, :olive_pollen, :ragweed_pollen, :pollen_total, :date_update)
                ON CONFLICT (camps_point_id, forecast_time, analysis_time) 
                DO UPDATE SET
                              alder_pollen = EXCLUDED.alder_pollen,
                              birch_pollen = EXCLUDED.birch_pollen,
                              grass_pollen = EXCLUDED.grass_pollen,
                              mugwort_pollen = EXCLUDED.mugwort_pollen,
                              olive_pollen = EXCLUDED.olive_pollen,
                              ragweed_pollen = EXCLUDED.ragweed_pollen,
                              pollen_total = EXCLUDED.pollen_total,
                              date_update = EXCLUDED.date_update 
                """)


        with connection.begin():
            coord_to_id = query_coord_to_id(connection)
            #print(coord_to_id)
            for _, row in df.iterrows():
                try:
                    camps_point_id = get_id_by_coord(connection, row["latitude"], row["longitude"], coord_to_id)
                    #print(f"Inserting pollen data for camps_point_id {camps_point_id} at {row['forecast_time']}")
                    connection.execute(sql, {
                        "camps_point_id": camps_point_id,
                        "forecast_time": row["forecast_time"],
                        "analysis_time": row["analysis_time"],
                        "alder_pollen": parse_numeric(row["apg_conc"]),
                        "birch_pollen": parse_numeric(row["bpg_conc"]),
                        "grass_pollen": parse_numeric(row["gpg_conc"]),
                        "mugwort_pollen": parse_numeric(row["mpg_conc"]),
                        "olive_pollen": parse_numeric(row["opg_conc"]),
                        "ragweed_pollen": parse_numeric(row["rwpg_conc"]),
                        "pollen_total": parse_numeric(row["pollen_total"]),
                        "date_update": datetime.now()
                    })
                except Exception as e:
                    logger.exception(
                        f"Error inserting pollen for lat={row['latitude']}, lon={row['longitude']} "
                        f"at {row['forecast_time']}: {e}" 
                        )

        current_date += timedelta(days=1)
        print(f"Finished processing pollen data for {date_str}")

            


if __name__ == "__main__":
    from db_config import get_database_url
    engine = create_engine(get_database_url())
    with engine.connect() as connection:

        insert_pollen_data(connection, "2026-03-01", "2026-03-02")
