import logging
from sqlalchemy import create_engine, text
import pollen
from parser import parse_numeric

logger = logging.getLogger(__name__)

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

        sql = text("""INSERT INTO public.pollen_data(latitude, longitude, forecast_time, analysis_time, alder_pollen, birch_pollen, grass_pollen, mugwort_pollen, olive_pollen, ragweed_pollen, pollen_total, date_update)
                VALUES (:latitude, :longitude, :forecast_time, :analysis_time, :alder_pollen, :birch_pollen, :grass_pollen, :mugwort_pollen, :olive_pollen, :ragweed_pollen, :pollen_total, :date_update)
                ON CONFLICT (latitude, longitude, forecast_time) 
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

        for _, row in df.iterrows():
            try:
                with connection.begin_nested():
                    connection.execute(sql, {
                        "latitude": row["latitude"],
                        "longitude": row["longitude"],
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
                logger.exception(f"Error inserting pollen for lat {row['latitude']}, lon {row['longitude']}, time {row['forecast_time']}: {e}")

        current_date += timedelta(days=1)


if __name__ == "__main__":
    from db_config import get_database_url
    engine = create_engine(get_database_url())
    with engine.begin() as connection:
        insert_pollen_data(connection, "2026-03-01", "2026-03-01")