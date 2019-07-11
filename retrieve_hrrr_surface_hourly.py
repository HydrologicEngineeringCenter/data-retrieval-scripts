import urllib.request
from datetime import datetime
import os

date = datetime.today().strftime('%Y%m%d')
cycle = 0

for hour in range(1, 37):
    url = "http://nomads.ncep.noaa.gov/pub/data/nccf/com/hrrr/prod/hrrr.{date}/conus/hrrr.t{:02d}z.wrfsfcf{:02d}.grib2".format(
        cycle, hour, date=date)
    print(url)

    filename = url.split("/")[-1]

    if not os.path.isdir("C:/Temp/hrrr/" + date + os.sep):
        os.mkdir("C:/Temp/hrrr/" + date + os.sep)
    f = open("C:/Temp/hrrr/" + date + os.sep + filename, 'wb')
    f.write(urllib.request.urlopen(url).read())
