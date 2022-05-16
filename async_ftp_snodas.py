# Tested on python 3.9
import pandas as pd
import asyncio
import aioftp
import os
import pathlib
import aiofiles
import logging
import pathlib
import aiofiles.os as aos
import async_timeout
import sys
from pprint import pprint

async def dowload_files(paths, dest_dir):
    """
    Function to dowload files from ftp server.
    Lots of error catching to ensure complete file
    download.

    Args:
        paths (list(pathlib.PurePath)): List of server paths to download
        dest_dir (pathlib.PurePath): Output Directory
    """    
    for path in paths:
        remote_size = None
        dest = dest_dir.joinpath(path.name)
        logging.info(f'File to download: {path} \n')
        logging.info(f'Should download to {dest} \n')
        max_attempts = 0

        while max_attempts<30:
            
            try:
                logging.info(f'Starting Attempt {max_attempts}')
                async with async_timeout.timeout(20):
                    async with aioftp.Client.context('128.138.135.20', user='anonymous', port=21) as client:
                        
                        if remote_size is None:
                            logging.info(f'Getting remote stats for file {path}')
                            remote_stat = await client.stat(path)
                            remote_size = int(remote_stat['size'])
                            logging.info(f' Remote file has size {remote_size}')
                        
                        async with aiofiles.open(dest, mode='ab', ) as local_file:
                            
                            #Check to see if local_file exists
                            if await aos.path.exists(dest):
                                stat = await aos.stat(dest)
                                size = stat.st_size
                            else:
                                size = 0
                            logging.info(f'Starting at postition {size}')
                            local_file.seek(size)

                            if remote_size == size:
                                break
                            elif size > remote_size:
                                pathlib.Path(dest).unlink()
                                logging.info('local file larger than remote file, removing now')
                                max_attempts +=1
                                size = 0

                            async with client.download_stream(path, offset=size) as stream:
                                async for block in stream.iter_by_block():
                                    await local_file.write(block)
                                    
            except aioftp.StatusCodeError as ftp_e:

                max_attempts +=1
                logging.info(f'Found aioftp error, trying another attempt')
                if ftp_e.received_codes ==( '426',):
                    logging.info(f'Forced timeout error, trying another attempt')

                if ftp_e.received_codes != ( '426',):
                    logging.info('new code')
                await asyncio.sleep(1)
                continue

            except asyncio.exceptions.TimeoutError as asy_e:
                logging.info(f'found time out exception')
                max_attempts +=1
                continue



async def list_files(month_year ):
    """
    Function to recursively search FTP for desired files.
    Args:
        month_year (list(str))): list of dates of interest (yyyymmdd)
    Returns:
        paths (list(pathlib.PurePath)): List of paths found to dowload
    """
  
    async with aioftp.Client.context('128.138.135.20', user='anonymous', port=21) as client:

        await client.change_directory(f'DATASETS/NOAA/G02158/masked/')
        _path = await client.get_current_directory()
        
        paths = []

        async for path, info in  client.list(_path, recursive=True):
  
            if "SNODAS" in path.name and path.suffix =='.tar' and path.name.split('_')[1][:-4] in month_year:
                paths.append(path)



    return paths
    
async def main( month_year, out_dir):

    paths = await list_files(month_year)

    pprint(paths)
    
    await dowload_files(paths, pathlib.PurePath(out_dir))

if __name__ == '__main__':

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
        datefmt="[%H:%M:%S]:",
    )

    start_date = '2008-10-01'
    end_date = '2010-10-01'
    drange = pd.date_range(start_date, end_date, freq ='D') 

    #list of dates to download
    month_year = [f'{i.strftime("%Y%m%d")}' for i in drange if i.month not in [8,9]]

    out_dir = f'output/SNODAS'

    os.makedirs(out_dir, exist_ok=True)

    loop = asyncio.get_event_loop()
    loop.set_debug(True)
    loop.run_until_complete(main(month_year, out_dir))