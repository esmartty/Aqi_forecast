from sqlalchemy import text

def fetch_last_historical_pollen_data_date(connection):

    sql = text("""
               SELECT MAX(analysis_time) as last_historical_date
	            FROM public.pollen_data;
            """)
    
    row = connection.execute(sql).mappings().first()

    last_historical_date = row["last_historical_date"] if row else None

    return last_historical_date
