import subprocess
from pathlib import Path
import json
from dataclasses import dataclass
from typing import Optional

import aiofiles
import aiohttp
import logging

from aiohttp import ClientResponseError

from drs_downloader.models import DrsClient, DrsObject, AccessMethod, Checksum

logger = logging.getLogger(__name__)


class TerraDrsClient(DrsClient):
    """
    Calls the terra DRS server.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.endpoint = "https://us-central1-broad-dsde-prod.cloudfunctions.net/martha_v3"
        self.token = self._get_auth_token()

    @dataclass
    class GcloudInfo(object):
        account: str
        project: str

    def _get_auth_token(self) -> str:
        """Get Google Cloud authentication token.
        User must run 'gcloud auth login' from the shell before starting this script.

        Returns:
            str: auth token
        """
        gcloud_info = self._get_gcloud_info()
        if gcloud_info.account is None:
            raise Exception("No Google Cloud account found.")

        token_command = "gcloud auth print-access-token"
        cmd = token_command.split(' ')
        token = subprocess.check_output(cmd).decode("ascii")[0:-1]
        assert token, "No token retrieved."
        return token

    def _get_gcloud_info(self) -> GcloudInfo:
        login_command = "gcloud info --format=json"
        cmd = login_command.split(' ')
        output = subprocess.check_output(cmd)
        js = json.loads(output)
        account = js['config']['account']
        project = js['config']['project']

        gcloud_info = self.GcloudInfo(account, project)
        return gcloud_info

    async def download_part(self,
                            drs_object: DrsObject, start: int, size: int, destination_path: Path) -> Optional[Path]:
        try:
            headers = {'Range': f'bytes={start}-{size}'}

            file_name = destination_path / f'{drs_object.name}.{start}.{size}.part'
            async with aiohttp.ClientSession(headers=headers) as session:
                async with session.get(drs_object.access_methods[0].access_url) as request:
                    file = await aiofiles.open(file_name, 'wb')
                    self.statistics.set_max_files_open()
                    async for data in request.content.iter_any():  # uses less memory
                        await file.write(data)
                    await file.close()
                    return Path(file_name)
        except Exception as e:
            logger.error(f"terra.download_part {str(e)}")
            drs_object.errors.append(str(e))
            return None

    async def sign_url(self, drs_object: DrsObject) -> DrsObject:
        """No-op.  terra returns a signed url in `get_object` """

        assert isinstance(drs_object, DrsObject), "A DrsObject should be passed"

        data = {
            "url": drs_object.id,
            "fields": ["accessUrl"]
        }
        session = aiohttp.ClientSession(headers={
            'authorization': 'Bearer ' + self.token,
            'content-type': 'application/json'
        })

        async with session.post(url=self.endpoint, json=data) as response:
            try:
                self.statistics.set_max_files_open()
                response.raise_for_status()
                resp = await response.json(content_type=None)
                assert 'accessUrl' in resp, resp
                if resp['accessUrl'] is None:
                    account_command = 'gcloud config get-value account'
                    cmd = account_command.split(' ')
                    account = subprocess.check_output(cmd).decode("ascii")
                    raise Exception(
                        f"A valid URL was not returned from the server.  Please check the access for {account}\n{resp}")
                url_ = resp['accessUrl']['url']
                drs_object.access_methods = [AccessMethod(access_url=url_, type='gs')]
                return drs_object
            except ClientResponseError as e:
                drs_object.errors.append(str(e))
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
        data = {
            "url": object_id,
            "fields": ["fileName", "size", "hashes"]
        }
        session = aiohttp.ClientSession(headers={
            'authorization': 'Bearer ' + self.token,
            'content-type': 'application/json'
        })

        async with session.post(url=self.endpoint, json=data) as response:
            try:
                self.statistics.set_max_files_open()
                response.raise_for_status()
                resp = await response.json(content_type=None)

                md5_ = resp['hashes']['md5']
                size_ = resp['size']
                name_ = resp['fileName']
                return DrsObject(
                    self_uri=object_id,
                    size=size_,
                    checksums=[Checksum(checksum=md5_, type='md5')],
                    id=object_id,
                    name=name_,
                    access_methods=[AccessMethod(access_url="", type='gs')]
                )
            except ClientResponseError as e:
                return DrsObject(
                    self_uri=object_id,
                    id=object_id,
                    checksums=[],
                    size=0,
                    name=None,
                    errors=[str(e)]
                )
            finally:
                await session.close()
