# -*- coding: utf-8 -*-
"""
Created on Wed Aug  5 13:49:42 2020

@author: RDCRLDDH
"""

from datetime import datetime
from datetime import timedelta
import os
import nest_asyncio
nest_asyncio.apply()
import asyncio
import aiohttp
import async_timeout


async def download_coroutine(url, session):
    with async_timeout.timeout(1200):
        async with session.get(url) as response:
            if response.status == 200:
                fp = r"C:\workspace\ririe\HMS\data\temp" + os.sep + os.path.basename(url)
                with open(fp, 'wb') as f_handle:
                    while True:
                        chunk = await response.content.read(1024)
                        if not chunk:
                            break
                        f_handle.write(chunk)
            else:
                print(url)
            return await response.release()

async def main(loop):
    
    start = datetime(2015, 1, 22, 0, 0)
    end = datetime(2020, 8, 1, 0, 0)
    hour = timedelta(hours=1)
    
    date = start
    urls = []
    opath = []
    while date < end:
        url = "http://mtarchive.geol.iastate.edu/{:04d}/{:02d}/{:02d}/grib2/ncep/RTMA/{:04d}{:02d}{:02d}{:02d}00_TMPK.grib2".format(
            date.year, date.month, date.day, date.year, date.month, date.day, date.hour)
        #print(url)
        destination = r"C:\workspace\ririe\HMS\data\temp"
        
        filename = url.split("/")[-1]
        if not os.path.isfile(destination + os.sep + filename):
            urls.append(url)
            opath.append(destination + os.sep + filename)
        date += hour
        
    async with aiohttp.ClientSession() as session:
        tasks = [download_coroutine(url, session) for url in urls]
        return await asyncio.gather(*tasks)




if __name__ == '__main__':

    loop = asyncio.get_event_loop()
    
    results = loop.run_until_complete(main(loop))
    