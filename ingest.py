from datetime import datetime
from models import WeatherRecord, WeatherStats, init_db
from sqlalchemy import func

import numpy as np
import pandas as pd

import logging
import glob
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def process_weather_file(filepath, session):
    """
    Process a single weather data file and save records to the database.
    
    Args:
        filepath: Path to the weather data file
        session: SQLAlchemy database session
    
    Returns:
        Number of records processed
    """
    # Extract station ID from filename
    station_id = os.path.basename(filepath).split('.')[0]
    
    # Read data file into DataFrame
    df = pd.read_csv(filepath, sep='\t', header=None,
                     names=['date', 'max_temp', 'min_temp', 'precipitation'])
    
    # Convert date strings to datetime objects
    df['date'] = pd.to_datetime(df['date'], format='%Y%m%d')
    
    # Replace missing values (-9999) with NaN and convert units
    # Temperature: tenths of degrees C to degrees C
    # Precipitation: tenths of mm to cm
    df['max_temp'] = df['max_temp'].replace(-9999, np.nan) / 10.0
    df['min_temp'] = df['min_temp'].replace(-9999, np.nan) / 10.0
    df['precipitation'] = df['precipitation'].replace(-9999, np.nan) / 10.0
    
    # Remove records with invalid dates
    df = df.dropna(subset=['date'])
    
    # Create database records
    records = []
    for _, row in df.iterrows():
        # Check for existing record
        existing_record = session.query(WeatherRecord).filter_by(
            station_id=station_id,
            date=row['date'].date()
        ).first()
        
        if existing_record:
            logger.info(f"Duplicate record found for station_id {station_id} on date {row['date'].date()}")
            continue
        
        record = WeatherRecord(
            station_id=station_id,
            date=row['date'].date(),
            max_temp=None if pd.isna(row['max_temp']) else float(row['max_temp']),
            min_temp=None if pd.isna(row['min_temp']) else float(row['min_temp']),
            precipitation=None if pd.isna(row['precipitation']) else float(row['precipitation'])
        )
        records.append(record)
    
    # Bulk insert records for better performance
    session.bulk_save_objects(records)
    return len(records)


def calculate_yearly_stats(session):
    """
    Calculate yearly statistics for all weather stations.
    
    Args:
        session: SQLAlchemy database session
    """
    # Clear existing statistics
    session.query(WeatherStats).delete()
    
    # Calculate new statistics using SQL aggregation
    stats = session.query(
        WeatherRecord.station_id,
        func.extract('year', WeatherRecord.date).label('year'),
        func.avg(WeatherRecord.max_temp).label('avg_max_temp'),
        func.avg(WeatherRecord.min_temp).label('avg_min_temp'),
        func.sum(WeatherRecord.precipitation).label('total_precipitation')
    ).group_by(
        WeatherRecord.station_id,
        func.extract('year', WeatherRecord.date)
    ).all()
    
    # Save statistics to database
    for stat in stats:
        weather_stat = WeatherStats(
            station_id=stat.station_id,
            year=int(stat.year),
            avg_max_temp=stat.avg_max_temp,
            avg_min_temp=stat.avg_min_temp,
            total_precipitation=stat.total_precipitation
        )
        session.add(weather_stat)

def main(data_dir='wx_data'):
    """
    Main function to process all weather data files and calculate statistics.
    
    Args:
        data_dir: Directory containing weather data files
    """
    start_time = datetime.now()
    logger.info(f"Starting data ingestion at {start_time}")
    
    Session = init_db()
    session = Session()
    
    try:
        # Process all weather data files
        total_records = 0
        for filepath in glob.glob(os.path.join(data_dir, '*.txt')):
            records_processed = process_weather_file(filepath, session)
            total_records += records_processed
            logger.info(f"Processed {records_processed} records from {filepath}")
            session.commit()
        
        # Calculate and save yearly statistics
        logger.info("Calculating yearly statistics...")
        calculate_yearly_stats(session)
        session.commit()
        
    except Exception as e:
        logger.error(f"Error during ingestion: {e}")
        session.rollback()
        raise
    finally:
        session.close()
    
    end_time = datetime.now()
    logger.info(f"Ingestion completed at {end_time}")
    logger.info(f"Total records processed: {total_records}")

if __name__ == '__main__':
    main()