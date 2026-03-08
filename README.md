# Multi-source-Climate-Trend-Data-Integration-Pipeline

## Project Description

The project is multi-level Big Data system stack that aims to process 21.9 million pieces of climate data from 2016 to the current year. It utilizes a distributed system that combines Apache Spark for parallel processing, the Hadoop Distributed File System (HDFS) for fault-tolerant storage, and MongoDB for analytical queries. The project's goal is to identify long-term trends for temperature and precipitation levels from 2,000 worldwide weather stations across the United States. 

## Data Source Identification
The system ingests heterogeneous data from three primary meteorological sources:

1. **National Oceanic and Atmospheric Administration (NOAA) Climate Data Online (CDO) API V2:** Primary source for historical daily summaries and global station metadata.
2. **National Weather Service (NWS) API:** Utilized for high-frequency observations and regional US climate data.
3. **Open-Meteo Historical Weather API:** Provides supplemental high-resolution global climate records.

## Setup Instructions

1. Clone the Repository
   git clone https://github.com/Aveon/Multi-source-Climate-Trend-Data-Integration-Pipeline.git
   cd Multi-source-Climate-Trend-Data-Integration-Pipeline

2. Create Python Virtual Environment
   python3 -m venv .venv
   source .venv/bin/activate

3. Install Dependencies
   pip install -r requirements.txt
   Required Libaries include: requests, pymongo, pyspark, and python-dotenv

## Environment Setup

This project requires a MongoDB connection string stored as an environmental variable
Create a .ennv file in the root project directory
MONGODB_URI=your_mongodb_connection_string_here

Optional configuration variables: (These values can also be configured in src/config/settings.py
MONGODB_DB=climate_data
MONGODB_COLLECTION=daily_observations
STATION=KATL

## How to Run Pipeline
From the project root directory run: python src/main.py

This will:
1. Retrieve weather observations from the National Weather Service API
2. Save raw JSON response locally
3. Store the observation records in MongoDB

Raw API data is saved locally at: data/raw/<date>/nws_raw.json

## Current Status

  # Completed
  Weather data acquisition from the National Weather Service API

  Raw JSON data storage for traceability

  MongoDB integration for observation storage

  Modular pipeline structure (ingestion → processing → storage)

  Environment configuration support using .env

  # In Progress
  Spark-based processing and aggregation of climate observations

  Additional data source integrations

  Creation of curated climate trend datasets
