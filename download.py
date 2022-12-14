
import aiohttp
import aiofiles

from pathlib import Path

from urllib.request import urlretrieve


class DownloadAsync(object):
    
    def __init__(
            self, 
            file : str,
            dpath: str    
        ):
        self._file = file
        self._download_path =  dpath

    async def _download(self, file_url, file_path):

        try:

            async with aiohttp.ClientSession() as session:

                async with session.get(file_url, timeout = None) as response:

                    async with aiofiles.open(file_path, 'wb') as fd:

                        while True:
                            chunk = await response.content.read(1024*8)
                            if not chunk:
                                break
                            await fd.write(chunk)                            

        except Exception as e:
            print("Error downloading %s %s ",file_url,e.args)


    async def __call__(self):

        file = Path(self._file)
        zpath = Path(self._download_path)
        zfile = zpath.joinpath(file.name)        

        print(f'downloading: {self._file}')

        await self._download(self._file, str(zfile))
        
        return str(zfile)
    
class Download(object):
    
    def __init__(
            self, 
            file : str,
            dpath: str    
        ):
        self._file = file
        self._download_path =  dpath


    def _download(self, file_url, file_path):

        try:

            urlretrieve(file_url, file_path)
            
        except Exception as e:
            print("Error downloading %s %s ",file_url,e.args)

    def __call__(self):

        file = Path(self._file)
        zpath = Path(self._download_path)
        zfile = zpath.joinpath(file.name)        

        print(f'downloading: {self._file}')

        self._download(self._file, str(zfile))
        
        return str(zfile)