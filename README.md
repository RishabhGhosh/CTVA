# Weather Data Processing and API

This project processes weather data from text files, stores it in a database, calculates statistics, and provides a REST API to access the data.

## Problem Solutions

### 1. Data Modeling
- Used SQLAlchemy ORM with SQLite database
- Created two main models:
  - `WeatherRecord`: Stores daily weather measurements (temperature, precipitation)
  - `WeatherStats`: Stores yearly statistics for each weather station
- Implemented unique constraints to prevent duplicate records

### 2. Data Ingestion
- Developed a robust ingestion script that processes raw text files
- Features include:
  - Duplicate detection and prevention
  - Unit conversion (temperature and precipitation)
  - Missing data handling (-9999 values)
  - Comprehensive logging
  - Bulk database operations for better performance

### 3. Data Analysis
- Implemented yearly statistics calculation for each weather station:
  - Average maximum/minimum temperatures
  - Total precipitation
- Statistics are stored in a separate table for efficient querying
- Null values are properly handled for incomplete data

### 4. REST API
- Built using FastAPI framework
- Endpoints:
  - `/api/weather`: Access raw weather records
  - `/api/weather/stats`: Access calculated statistics
- Features:
  - Pagination
  - Filtering by date and station ID
  - Automatic OpenAPI documentation
  - Type validation using Pydantic models

## Setup and Installation

### Prerequisites
- Python 3.8 or higher
- pip package manager

### Installation Steps

1. Clone the repository:
```bash
git clone <repository-url>
cd weather-data-project
```

2. Create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

### Running the Application

1. Process weather data:
```bash
python ingest.py
```

2. Start the API server:
```bash
uvicorn api:app --reload
```

OpenAPI can be accessed at `http://localhost:8000/docs`

### Running Tests
```bash
pytest test_ctva_app.py -v
```

## Project Structure
```
weather-data-project/
├── api.py           # FastAPI application and endpoints
├── ingest.py        # Data ingestion script
├── models.py        # Database models and initialization
├── test_ctva_app.py # Test suite
├── requirements.txt # Project dependencies
└── wx_data/        # Directory for weather data files
```

## API Usage Examples

1. Get weather records:
```bash
curl "http://localhost:8000/api/weather?station_id=USC00110072&start_date=2020-01-01"
```

2. Get weather statistics:
```bash
curl "http://localhost:8000/api/weather/stats?year=2020"
```

## Deployment Considerations (AWS)

For AWS deployment, consider:
- Database: Amazon RDS (PostgreSQL) or Amazon Aurora
- API Hosting: AWS Elastic Beanstalk or ECS/Fargate
- Scheduled Ingestion: AWS Lambda with CloudWatch Events
- Infrastructure as Code: AWS CDK or Terraform
- CI/CD: AWS CodePipeline with GitHub Actions
- Monitoring: CloudWatch Logs and Metrics
