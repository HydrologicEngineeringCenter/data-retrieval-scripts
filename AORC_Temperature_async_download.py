# -*- coding: utf-8 -*-
from datetime import datetime
from datetime import timedelta
import os
import nest_asyncio
nest_asyncio.apply()
import asyncio
import aiohttp
import async_timeout
from dateutil.relativedelta import relativedelta as relativeDelta

async def download_coroutine(url, session, destination):
    async with async_timeout.timeout(1200):
        async with session.get(url) as response:
            if response.status == 200:
                fp = destination + os.sep + os.path.basename(url)
                with open(fp, 'wb') as f_handle:
                    while True:
                        chunk = await response.content.read(1024)
                        if not chunk:
                            break
                        f_handle.write(chunk)
            else:
                print(url)
            return await response.release()

async def main(loop, tmp, destination):

    async with aiohttp.ClientSession() as session:
        tasks = [download_coroutine(url, session, destination) for url in tmp]
        return await asyncio.gather(*tasks)


if __name__ == '__main__':

    start = datetime(2021, 10, 1, 0, 0)
    end = datetime(2021, 12, 1, 0, 0)
    destination = r"D:\FY22\SanLorenzo\data\AORC"
    office = 'CNRFC'

    hour = relativeDelta(months=1)
    os.makedirs(destination, exist_ok=True)
    
    #loop through and see if you already have the file locally
    date = start
    urls = []
    opath = []
    while date <= end:
        
        #need date like 197902
        url = f"https://hydrology.nws.noaa.gov/aorc-historic/AORC_{office}_4km/AORC_TMPR_4KM_{office}_{date.strftime('%Y%m')}.zip"

        filename = url.split("/")[-1]
        if not os.path.isfile(destination + os.sep + filename):
            urls.append(url)
            opath.append(destination + os.sep + filename)
        date += hour

    #Split urls into chunks so you wont overwhelm IA mesonet with asyncronous downloads
    chunk_size = 50
    chunked_urls = [urls[i * chunk_size:(i + 1) * chunk_size] for i in range((len(urls) + chunk_size - 1) // chunk_size )]

    for tmp in chunked_urls:
        loop = asyncio.get_event_loop()
        results = loop.run_until_complete(main(loop, tmp, destination))
        del loop, results
