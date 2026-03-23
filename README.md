# Multi-source-Climate-Trend-Data-Integration-Pipeline

## Project Description

The project is multi-level Big Data system stack that aims to process 21.9 million pieces of climate data from 2016 to the current year. It utilizes a distributed system that combines Apache Spark for parallel processing, the Hadoop Distributed File System (HDFS) for fault-tolerant storage, and MongoDB for analytical queries. The project's goal is to identify long-term trends for temperature and precipitation levels from 2,000 worldwide weather stations across the United States. 

## Data Source Identification
The system ingests heterogeneous data from three primary meteorological sources:

1. **National Oceanic and Atmospheric Administration (NOAA) Climate Data Online (CDO) API V2:** Primary source for historical daily summaries and global station metadata.
2. **National Weather Service (NWS) API:** Utilized for high-frequency observations and regional US climate data.
3. **Open-Meteo Historical Weather API:** Provides supplemental high-resolution global climate records.
