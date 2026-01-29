# Taxi Zone Lookup Data
# This file will be replaced by the actual NYC TLC CSV

# Download from:
# https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv

# Expected format:
# LocationID,Borough,Zone,service_zone
# 1,EWR,Newark Airport,EWR
# 2,Queens,Jamaica Bay,Boro Zone
# ...

# Download command:
# wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/misc/taxi_zone_lookup.csv \
#   -O taxi_rides_ny/seeds/taxi_zone_lookup.csv

# Then load to BigQuery:
# dbt seed
