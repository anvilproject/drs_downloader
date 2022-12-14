import json
import logging
import os
from pathlib import Path

import aiofiles
import aiohttp

from drs_downloader.models import DrsClient, DrsObject, AccessMethod, Checksum

logger = logging.getLogger(__name__)


class Gen3DrsClient(DrsClient):
    """
    Calls the Gen3 DRS server indexd
    """

    def __init__(self, api_key_path, endpoint,
                 access_token_resource_path='/user/credentials/cdis/access_token',
                 drs_api='/ga4gh/drs/v1/objects/',
                 *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.authorized = None
        self.api_key = None
        self.endpoint = endpoint
        self.token = None
        self.access_token_resource_path = access_token_resource_path
        self.api_key_path = api_key_path
        self.drs_api = drs_api

    async def authorize(self):
        full_key_path = os.path.expanduser(self.api_key_path)
        try:
            with open(full_key_path) as f:
                self.api_key = json.load(f)
            code = await self.update_access_token()
            if code == 401:
                print('Invalid access token in {}'.format(full_key_path))
                self.api_key = None
            elif code != 200:
                print('Error {} getting Access token for {}'.format(code, self.endpoint))
                print('Using {}'.format(full_key_path))
                self.api_key = None
        except Exception as e:
            raise e
            self.api_key = None

    # Obtain an access_token using the provided Fence API key.
    # The client object will retain the access key for subsequent calls
    async def update_access_token(self):
        headers = {'Content-Type': 'application/json'}
        api_url = '{0}{1}'.format(self.endpoint, self.access_token_resource_path)
        async with aiohttp.ClientSession(headers=headers) as session:
            response = await session.post(api_url, headers=headers, json=self.api_key)
            if response.status == 200:
                resp = await response.json()
                self.token = resp['access_token']
                self.authorized = True
            else:
                self.authorized = False
            return response.status

    async def download_part(self, drs_object: DrsObject, start: int, size: int, destination_path: Path) -> Path:
        try:

            if not self.authorized:
                await self.authorize()

            headers = {'Range': f'bytes={start}-{size}'}

            file_name = destination_path / f'{drs_object.name}.{start}.{size}.part'
            Path(file_name).parent.mkdir(parents=True, exist_ok=True)

            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.get(drs_object.access_methods[0].access_url) as request:
                    file = await aiofiles.open(file_name, 'wb')
                    self.statistics.set_max_files_open()
                    async for data in request.content.iter_any():  # uses less memory
                        await file.write(data)
                    await file.close()
                    return Path(file_name)
        except Exception as e:
            logger.error(f"gen3.download_part {str(e)}")
            drs_object.errors.append(str(e))
            return None

    async def sign_url(self, drs_object: DrsObject) -> DrsObject:
        """Call fence's /user/data/download/ endpoint. """

        session = aiohttp.ClientSession(headers={
            'authorization': 'Bearer ' + self.token,
            'content-type': 'application/json'
        })

        async with session.get(url=f"{self.endpoint}/user/data/download/{drs_object.id.split(':')[-1]}") as response:
            try:
                self.statistics.set_max_files_open()
                response.raise_for_status()
                resp = await response.json(content_type=None)
                assert 'url' in resp, resp
                url_ = resp['url']
                drs_object.access_methods = [AccessMethod(access_url=url_, type='s3')]
                return drs_object
            finally:
                await session.close()

    async def get_object(self, object_id: str) -> DrsObject:
        """Sends a POST request for the signed URL, hash, and file size of a given DRS object.

        Args:
            object_id (str): DRS URI

        Raises:
            Exception: The request was rejected by the server

        Returns:
            DownloadURL: The downloadable bundle ready for async download
        """
        if not self.authorized:
            await self.authorize()

        session = aiohttp.ClientSession(headers={
            'authorization': 'Bearer ' + self.token,
            'content-type': 'application/json'
        })

        async with session.get(url=f"{self.endpoint}{self.drs_api}/{object_id.split(':')[-1]}") as response:
            try:
                self.statistics.set_max_files_open()
                response.raise_for_status()
                resp = await response.json(content_type=None)

                assert resp['checksums'][0]['type'] == 'md5', resp
                md5_ = resp['checksums'][0]['checksum']
                size_ = resp['size']
                name_ = resp['name']
                return DrsObject(
                    self_uri=object_id,
                    size=size_,
                    checksums=[Checksum(checksum=md5_, type='md5')],
                    id=object_id,
                    name=name_,
                    access_methods=[AccessMethod(access_url="", type='gs')]
                )
            finally:
                await session.close()
