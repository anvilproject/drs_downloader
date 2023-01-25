import subprocess
from pathlib import Path
import json
from dataclasses import dataclass
from typing import Optional

import aiofiles
import aiohttp
import logging

from aiohttp import ClientResponseError, ClientConnectorError

from drs_downloader.models import DrsClient, DrsObject, AccessMethod, Checksum

logger = logging.getLogger(__name__)
logging.getLogger().setLevel(logging.INFO)


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

        token_command = "gcloud auth application-default print-access-token"
        cmd = token_command.split(' ')
        try:
            token = subprocess.check_output(cmd).decode("ascii")[0:-1]
        except FileNotFoundError:
            logger.error("gcloud not found")
            exit(1)
        assert token, "No token retrieved."
        return token

    def _get_gcloud_info(self) -> GcloudInfo:
        login_command = "gcloud info --format=json"
        cmd = login_command.split(' ')
        output = subprocess.check_output(cmd)
        # logger.info("GCLOUD INFO %s",output)
        if "google-cloud-sdk" in str(output):
            logger.info("google-cloud-sdk credentials working")
        else:
            logger.info("google credentials are broken")

        js = json.loads(output)

        account = js['config']['account']
        project = js['config']['project']

        gcloud_info = self.GcloudInfo(account, project)
        return gcloud_info

    async def download_part(self,
                            drs_object: DrsObject, start: int, size: int, destination_path: Path) -> Optional[Path]:
        tries = 0
        while True:
            try:
                headers = {'Range': f'bytes={start}-{size}'}
                file_name = destination_path / f'{drs_object.name}.{start}.{size}.part'
                async with aiohttp.ClientSession(headers=headers) as session:
                    async with session.get(drs_object.access_methods[0].access_url) as request:
                        if (request.status > 399):
                            text = await request.content.read()

                        request.raise_for_status()

                        file = await aiofiles.open(file_name, 'wb')
                        self.statistics.set_max_files_open()
                        async for data in request.content.iter_any():  # uses less memory
                            await file.write(data)
                        await file.close()
                        return Path(file_name)

            except aiohttp.ClientResponseError as f:
                tries += 1
                if tries > 3:
                    logger.info(f"Error Text Body {str(text)}")
                    if ("The provided token has expired" in str(text)):
                        drs_object.errors.append(f'RECOVERABLE in AIOHTTP {str(f)}')
                        return None

            except Exception as e:
                tries += 1
                if tries > 3:
                    logger.info(f"Miscellaneous Error {str(text)}")
                    drs_object.errors.append(f'NONRECOVERABLE ERROR {str(e)}')
                    return None
        """
        import random
        numn =random.randint(0,9)
        logger.info(numn)
        if(numn>7):
            drs_object.errors.append(f'RECOVERABLE in AIOHTTP')
            return None
        else:
            drs_object.errors.append(f'fdssfsdff in AIOHdsfsdTTP')
            return None
        """

    async def sign_url(self, drs_object: DrsObject) -> DrsObject:
        """No-op.  terra returns a signed url in `get_object` """

        assert isinstance(drs_object, DrsObject), "A DrsObject should be passed"

        data = {
            "url": drs_object.id,
            "fields": ["accessUrl"]
        }
        headers = {
            'authorization': 'Bearer ' + self.token,
            'content-type': 'application/json'
        }
        tries = 0
        async with aiohttp.ClientSession(headers=headers) as session:
            while (True):  # This is here so that URL signing errors are caught they are rare, but I did capture one
                try:
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
                                    f"A valid URL was not returned from the server. \
                                    Please check the access for {account}\n{resp}")
                            url_ = resp['accessUrl']['url']
                            drs_object.access_methods = [AccessMethod(access_url=url_, type='gs')]
                            return drs_object
                        except ClientResponseError as e:
                            drs_object.errors.append(str(e))
                            logger.error(f"A file has failed the signing process, specifically {str(e)}")
                            return drs_object

                except ClientConnectorError as e:
                    logger.info("URL Signing Failed, retrying")
                    if (tries > 4):
                        logger.error("File download failure. \
    Run the exact command again to only download the missing file")
                        return DrsObject(
                                    self_uri=None,
                                    id=drs_object.id,
                                    checksums=[],
                                    size=0,
                                    name=None,
                                    errors=[str(e)]
                                )
                    else:
                        tries += 1

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
        headers = {
            'authorization': 'Bearer ' + self.token,
            'content-type': 'application/json'
        }
        tries = 0
        async with aiohttp.ClientSession(headers=headers) as session:
            while (True):  # this is here for the somewhat more common Martha disconnects.
                try:
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
                                name=name_)
                        except ClientResponseError as e:
                            return DrsObject(
                                self_uri=object_id,
                                id=object_id,
                                checksums=[],
                                size=0,
                                name=None,
                                errors=[str(e)]
                            )
                except ClientConnectorError as e:
                    logger.info("Martha Disconnect, retrying")
                    if (tries > 4):
                        logger.error("File download failure. \
    Run the exact command again to only download the missing file")
                        return DrsObject(
                                    self_uri=object_id,
                                    id=object_id,
                                    checksums=[],
                                    size=0,
                                    name=None,
                                    errors=[str(e)]
                                )
                    else:
                        tries += 1
