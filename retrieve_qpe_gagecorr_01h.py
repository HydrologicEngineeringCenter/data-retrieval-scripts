import urllib.request
from urllib.request import HTTPError
from datetime import datetime
from datetime import timedelta
import os

start = datetime(2018, 9, 1, 0, 0)
end = datetime(2018, 10, 1, 0, 0)
hour = timedelta(hours=1)

missing_dates = []
fallback_to_radaronly = True #Enables a post-processing step that will go through the list of missing dates for gage-corrected
############################# and tries to go get the radar-only values if they exist.

destination = "C:/Temp/qpe"

date = start

while date <= end:
    url = "http://mtarchive.geol.iastate.edu/{:04d}/{:02d}/{:02d}/mrms/ncep/GaugeCorr_QPE_01H/GaugeCorr_QPE_01H_00.00_{:04d}{:02d}{:02d}-{:02d}0000.grib2.gz".format(
        date.year, date.month, date.day, date.year, date.month, date.day, date.hour)
    filename = url.split("/")[-1]
    try:
        fetched_request = urllib.request.urlopen(url)
    except HTTPError as e:
        missing_dates.append(date)
    else:
        with open(destination + os.sep + filename, 'wb') as f:
            f.write(fetched_request.read())
    finally:
        date += hour

if fallback_to_radaronly:
    radar_also_missing = []
    for date in missing_dates:
        url = "http://mtarchive.geol.iastate.edu/{:04d}/{:02d}/{:02d}/mrms/ncep/RadarOnly_QPE_01H/RadarOnly_QPE_01H_00.00_{:04d}{:02d}{:02d}-{:02d}0000.grib2.gz".format(
            date.year, date.month, date.day, date.year, date.month, date.day, date.hour)
        filename = url.split("/")[-1]
        try:
            fetched_request = urllib.request.urlopen(url)
        except HTTPError as e:
            radar_also_missing.append(date)
        else:
            with open(destination + os.sep + filename, 'wb') as f:
                f.write(fetched_request.read())
