import urllib.request
from urllib.request import HTTPError
from datetime import datetime
from datetime import timedelta
import os

start = datetime(2017, 1, 1, 0, 0)
end = datetime(2017, 1, 3, 0, 0)
hour = timedelta(hours=1)

missing_dates = []

destination = "C:/Temp"

date = start

while date < end:
    url = "http://mtarchive.geol.iastate.edu/{:04d}/{:02d}/{:02d}/grib2/ncep/RTMA/{:04d}{:02d}{:02d}{:02d}00_TMPK.grib2".format(
        date.year, date.month, date.day, date.year, date.month, date.day, date.hour)
    filename = url.split("/")[-1]
    try:
        fetched_request = urllib.request.urlopen(url)
        print("")
        print("opening: " + url)
    except HTTPError as e:
        missing_dates.append(date)
    else:
        with open(destination + os.sep + filename, 'wb') as f:
            f.write(fetched_request.read())
    finally:
        date += hour