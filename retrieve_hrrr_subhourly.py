import urllib.request
from datetime import datetime
import os

date = datetime.today().strftime('%Y%m%d')
cycle = 0

for hour in range(1, 19):
    url = "http://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod/hrrr.{date}/conus/hrrr.t{:02d}z.wrfsubhf{:02d}.grib2".format(cycle, hour, date=date)
    print(url)

    filename = url.split("/")[-1]
    destination = "C:/Temp/hrrr/" + date + os.sep + "{:02d}".format(cycle)
    if not os.path.isdir(os.path.split(destination)[0]):
        os.mkdir(os.path.split(destination)[0])
    if not os.path.isdir(destination):
        os.mkdir(destination)
    f = open(destination + os.sep + filename, 'wb')
    f.write(urllib.request.urlopen(url).read())
