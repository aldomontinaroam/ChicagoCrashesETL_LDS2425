# CRASHES
The dataset contains 257925 records with 36 attributes with information about road incidents in Chicago between January 18, 2014 and January 12, 2019. Data types: int, float and object (mainly strings but also datetimes formatted as strings). There are no duplicate rows.

<u>Missing values</u>:
1. **REPORT_TYPE**: binary column (ON SCENE / NOT ON SCENE(DESK REPORT)) which contains 1.94% of nulls.
2. **STREET_DIRECTION**: e.g., 'S' for South, contains 2 nulls.
3. **STREET_NAME**: e.g., 'DIVERSEY AVE', contains only 1 null. Street names are publicly available and can be retrieved online from various sources like maps or city databases.
4. **BEAT_OF_OCCURRENCE**: 4 nulls, may not be available online because may refer to specific data coming from police database.
5. **MOST_SEVERE_INJURY**: 7 nulls, not available online.
6. **LATITUDE** and **LONGITUDE**: both containing 0.4% of nulls but represent geographical coordinates, so they can be retrieved online from mapping services.
7. **LOCATION**: 1022 nulls, contains location data in the form of geographical coordinates (e.g., 'POINT (-87.712665377938 41.939731753033)') which can be retrieved online.

We will try to get missing data for street direction, street names, lat-lng and location from external accessible sources online.

<u>Insights</u>:
- Looking at the distributions of days and hours of accidents we see that most of the accidents recorded in this dataset occurred on a Friday, in October and around 3/4 PM
- A candidate identifier for records is the column "RD_NO" which contains 257925 unique values with equal freq 1 and constant length 8.
- The overall number of missing cells is 8076.
- In 79.65% of cases weather is CLEAR and in 65.65% of cases lighting is DAYLIGHT, with most of the accidents happened on DRY roadway surface.
- In most of the reported crashes there's no indication of the injury.
- The location recorded in the dataset with the highest number of records is 'POINT (-87.905309125103 41.976201139024)' which corresponds to a location near Chicago-O'Hare International Airport.

<u>Next steps</u>:
- Data cleaning: retrieve missing values of location columns using ´geopy´
- Data integration: additional data (hierarchical GeoHash/UberH3/GoogleS2 encoding for spatial data, properties of the road, additional weather conditions, etc.)