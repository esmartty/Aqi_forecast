import logging

from sqlalchemy import create_engine

from db_config import get_database_url
import loader_synop_data
import loader_station_data
import loader_aqi_historical_data
import loader_forecast_data

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
)
logger = logging.getLogger(__name__)

engine = create_engine(get_database_url())

def run_loader(loader_func, *args):
    try:
        with engine.begin() as connection:
            logger.info(f"{loader_func.__name__} transaction started")

            loader_func(connection, *args)

        logger.info(f"{loader_func.__name__} transaction committed successfully")

    except Exception as e:
        logger.exception(f"{loader_func.__name__} transaction rolled back due to error: {e}")

def main():
    station_id_list = ['3401', '3402', '3403', '3407', '8691', '9039', '14683']

    run_loader(loader_synop_data.insert_synop_data)
    run_loader(loader_station_data.insert_stations, station_id_list)
    run_loader(loader_aqi_historical_data.insert_aqi_data, station_id_list)
    run_loader(loader_forecast_data.insert_forecast_data, station_id_list)


#-----------------------------------
if __name__ == "__main__":
    main()