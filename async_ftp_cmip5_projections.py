# Tested on python 3.9

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
        paths (list(pathlib.PurePath)): List of paths to download
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
                    async with aioftp.Client.context('192.12.137.7', user='anonymous', port=21) as client:
                        
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



async def list_files(model, var, rcp ):
    """
    Function to recursively search FTP for desired files.
    Args:
        model (str): CMIP5 GCM Model name
        var (str): Variable to download ['pr','tasmax','tasmin','DTR']
        rcp (str): rcp name ['rcp45', 'rcp85']
    Returns:
        paths (list(pathlib.PurePath)): List of paths found to dowload
    """
  
    async with aioftp.Client.context('192.12.137.7', user='anonymous', port=21) as client:

        await client.change_directory(f'pub/dcp/archive/cmip5/loca/LOCA_2016-04-02/{model}')#/archive/cmip5/loca/LOCA_2016-04-02/
        _path = await client.get_current_directory()
        
        paths = []

        async for path, info in  client.list(_path, recursive=True):
  
            if rcp in path.name and '16th' in path.name and var in path.name and path.suffix =='.nc':
                paths.append(path)



    return paths
    
async def main( model, var, rcp, out_dir):

    paths = await list_files(model, var, rcp)

    pprint(paths)
    
    await dowload_files(paths, pathlib.PurePath(out_dir))

if __name__ == '__main__':

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(name)s] %(message)s",
        datefmt="[%H:%M:%S]:",
    )
    
    model ='ACCESS1-3'
    print(model)

    for rcp in ['rcp45', 'rcp85']:
        for var in ['pr','tasmin','tasmax']:

            out_dir = f'output/{model}/{rcp}/{var}'

            os.makedirs(out_dir, exist_ok=True)

            loop = asyncio.get_event_loop()
            loop.set_debug(True)
            loop.run_until_complete(main(model, var, rcp, out_dir))