import urllib.request
from datetime import datetime
from datetime import timedelta
import os

start = datetime(2017, 1, 1, 0, 0)
end = datetime(2017, 1, 3, 0, 0)
hour = timedelta(hours=1)

date = start
while date < end:
    url = "http://mtarchive.geol.iastate.edu/{:04d}/{:02d}/{:02d}/grib2/ncep/RTMA/{:04d}{:02d}{:02d}{:02d}00_TMPK.grib2".format(
        date.year, date.month, date.day, date.year, date.month, date.day, date.hour)
    print(url)
    destination = "C:/Temp"
    filename = url.split("/")[-1]
    f = open(destination + os.sep + filename, 'wb')
    f.write(urllib.request.urlopen(url).read())
    date += hour
