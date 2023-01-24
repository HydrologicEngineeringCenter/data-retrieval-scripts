import os
import logging
from datetime import datetime
import asyncio
import aiohttp
import async_timeout
import nest_asyncio
nest_asyncio.apply()

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
    session_timeout = aiohttp.ClientTimeout(total=None)

    async with aiohttp.ClientSession(timeout=session_timeout) as session:
        tasks = [download_coroutine(url, session, destination) for url in tmp]
        return await asyncio.gather(*tasks)


if __name__ == '__main__':
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
        datefmt="[%H:%M:%S]:",
    )
    
    start = datetime(1972, 1, 1, 0, 0)
    end = datetime(2018, 1,1, 0, 0)
    yearIncrement = 1
    outRoot = r"C:\workspace\prospectHmsAdvanced\output"

    variables = ['tmin','tmax','prec']

    for variable in variables:
        destination = rf'{outRoot}\{variable}'
        os.makedirs(destination, exist_ok=True)

        #loop through and see if you already have the file locally
        date = start
        urls = []
        opath = []
        while date < end:
            url = f"https://psl.noaa.gov/thredds/fileServer/Datasets/livneh/metvars/{variable}.{date.year}.nc"

            filename = url.split("/")[-1]
        # if not os.path.isfile(destination + os.sep + filename):
            urls.append(url)
            opath.append(destination + os.sep + filename)
            date = datetime(date.year + yearIncrement, date.month, date.day)

        #Split urls into chunks so you wont overwhelm server
        chunk_size = 3
        chunked_urls = [urls[i * chunk_size:(i + 1) * chunk_size] for i in range((len(urls) + chunk_size - 1) // chunk_size )]

        for tmp in chunked_urls:
            loop = asyncio.get_event_loop()
            loop.set_debug(True)
            results = loop.run_until_complete(main(loop, tmp, destination))
            del loop, results
