import os

MONGO_URI = os.getenv("MONGODB_URI") or os.getenv("MONGO_URI")
MONGO_DB = os.getenv("MONGODB_DB", "climate_data")
MONGO_COLLECTION = os.getenv("MONGODB_COLLECTION", "daily_observations")

DEFAULT_STATION = os.getenv("STATION", "KATL")  # Atlanta Hartsfield-Jackson Intl Airport
USER_AGENT = os.getenv("USER_AGENT", "BigDataProject/1.0 (joshua.young96@gmail.com)")
