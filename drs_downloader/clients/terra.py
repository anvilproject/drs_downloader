import subprocess
from pathlib import Path
from dataclasses import dataclass
from typing import Optional

import aiofiles
import aiohttp
import certifi
import ssl
import logging
import google.auth.transport.requests
import time
import random

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
        self.endpoint = (
            "https://us-central1-broad-dsde-prod.cloudfunctions.net/martha_v3"
        )
        self.token = None

    @dataclass
    class GcloudInfo(object):
        account: str
        project: str

    async def _get_auth_token(self) -> str:
        """Get Google Cloud authentication token.
        User must run 'gcloud auth login' from the shell before starting this script.

        Returns:
            str: auth token
            see https://github.com/DataBiosphere/terra-notebook-utils/blob/b53bb8656d
            502ecbdbfe9c5edde3fa25bd90bbf8/terra_notebook_utils/gs.py#L25-L42

        """

        creds, projects = google.auth.default()

        creds.refresh(google.auth.transport.requests.Request())

        token = creds.token

        assert token, "No token retrieved."
        logger.info("gcloud token successfully fetched")
        return creds

    async def download_part(
        self, drs_object: DrsObject, start: int, size: int, destination_path: Path, verbose: bool = False
    ) -> Optional[Path]:
        tries = 0
        while True:
            try:
                headers = {"Range": f"bytes={start}-{size}"}
                file_name = destination_path / f"{drs_object.name}.{start}.{size}.part"
                context = ssl.create_default_context(cafile=certifi.where())
                async with aiohttp.ClientSession(headers=headers) as session:
                    async with session.get(
                        drs_object.access_methods[0].access_url, ssl=context
                    ) as request:
                        if request.status > 399:
                            text = await request.content.read()

                        request.raise_for_status()

                        file = await aiofiles.open(file_name, "wb")
                        self.statistics.set_max_files_open()
                        async for data in request.content.iter_any():  # uses less memory
                            await file.write(data)
                        await file.close()
                        return Path(file_name)

            except aiohttp.ClientResponseError as f:
                tries += 1
                if "The provided token has expired" in str(text):
                    if verbose:
                        logger.info(f"Error Text Body {str(text)}")
                    drs_object.errors.append(f"RECOVERABLE in AIOHTTP {str(f)}")
                    return None

                time.sleep((random.randint(0, 1000) / 1000) + 2**tries)
                if tries > 2:
                    if verbose:
                        logger.info(f"Error Text Body {str(text)}")
                        return None

            except Exception as e:
                tries += 1
                time.sleep((random.randint(0, 1000) / 1000) + 2**tries)
                if tries > 2:
                    if verbose:
                        logger.info(f"Miscellaneous Error {str(text)}")
                    drs_object.errors.append(f"NONRECOVERABLE ERROR {str(e)}")
                    return None

    async def sign_url(self, drs_object: DrsObject, verbose: bool) -> DrsObject:
        """No-op.  terra returns a signed url in `get_object`"""

        assert isinstance(drs_object, DrsObject), "A DrsObject should be passed"

        if (self.token is None or (self.token.expired and self.token.expiry is not None)):
            if verbose:
                logger.info("fetching new token")
            self.token = await self._get_auth_token()

        if verbose:
            logger.info(f"status of token expiration {self.token.expiry}")

        data = {"url": drs_object.id, "fields": ["accessUrl"]}
        headers = {
            "authorization": "Bearer " + self.token.token,
            "content-type": "application/json",
        }
        tries = 0
        context = ssl.create_default_context(cafile=certifi.where())
        async with aiohttp.ClientSession(headers=headers) as session:
            while (
                True
            ):  # This is here so that URL signing errors are caught they are rare, but I did capture one
                try:
                    async with session.post(url=self.endpoint, json=data, ssl=context) as response:
                        while (True):
                            try:
                                self.statistics.set_max_files_open()

                                # these lines produced an error saying that the content.read() had already closed
                                if response.status > 399:
                                    text = await response.content.read()

                                response.raise_for_status()
                                resp = await response.json(content_type=None)
                                assert "accessUrl" in resp, resp
                                if resp["accessUrl"] is None:
                                    account_command = "gcloud config get-value account"
                                    cmd = account_command.split(" ")
                                    account = subprocess.check_output(cmd).decode("ascii")
                                    raise Exception(
                                        f"A valid URL was not returned from the server. \
                                        Please check the access for {account}\n{resp}"
                                    )
                                url_ = resp["accessUrl"]["url"]
                                type = "none"
                                if "storage.googleapis.com" in url_:
                                    type = "gs"

                                drs_object.access_methods = [
                                    AccessMethod(access_url=url_, type=type)
                                ]
                                return drs_object
                            except ClientResponseError as e:
                                tries += 1
                                if self.token.expired and self.token.expiry is not None:
                                    self.token = await self._get_auth_token()
                                time.sleep((random.randint(0, 1000) / 1000) + 2**tries)
                                if tries > 2:
                                    if verbose:
                                        logger.error(f"value of text error  {str(text)}")
                                        logger.error(f"A file has failed the signing process, specifically {str(e)}")
                                        if "401" in str(e):
                                            drs_object.errors.append(f"RECOVERABLE in AIOHTTP {str(e)}")

                                    return DrsObject(
                                        self_uri="",
                                        id="",
                                        checksums=[],
                                        size=0,
                                        name=None,
                                        errors=[f"error: {str(text)}"],
                                    )

                except ClientConnectorError as e:
                    tries += 1
                    if self.token.expired and self.token.expiry is not None:
                        self.token = await self._get_auth_token()
                    time.sleep((random.randint(0, 1000) / 1000) + 2**tries)
                    if tries > 2:
                        drs_object.errors.append(str(e))
                        if verbose:
                            logger.error(f"retry failed in sign_url function. Exiting with error status: {str(e)}")
                            return DrsObject(
                                self_uri="",
                                id="",
                                checksums=[],
                                size=0,
                                name=None,
                                errors=[f"error: {str(text)}"],
                            )

    async def get_object(self, object_id: str, verbose: bool = False) -> DrsObject:
        """Sends a POST request for the signed URL, hash, and file size of a given DRS object.

        Args:
            object_id (str): DRS URI

        Raises:
            Exception: The request was rejected by the server

        Returns:
            DownloadURL: The downloadable bundle ready for async download
        """

        if (self.token is None or (self.token.expired and self.token.expiry is not None)):
            if verbose:
                logger.info("fetching new token")
            self.token = await self._get_auth_token()

        if verbose:
            logger.info(f"status of token expiration {self.token.expiry}")

        data = {"url": object_id, "fields": ["fileName", "size", "hashes"]}
        headers = {
            "authorization": "Bearer " + self.token.token,
            "content-type": "application/json",
        }
        tries = 0

        context = ssl.create_default_context(cafile=certifi.where())
        async with aiohttp.ClientSession(headers=headers) as session:
            while True:  # this is here for the somewhat more common Martha disconnects.
                try:
                    async with session.post(url=self.endpoint, json=data, ssl=context) as response:
                        while True:
                            try:
                                self.statistics.set_max_files_open()
                                if response.status > 399:
                                    text = await response.content.read()

                                response.raise_for_status()
                                resp = await response.json(content_type=None)
                                md5_ = resp["hashes"]["md5"]
                                size_ = resp["size"]
                                name_ = resp["fileName"]
                                return DrsObject(
                                    self_uri=object_id,
                                    size=size_,
                                    checksums=[Checksum(checksum=md5_, type="md5")],
                                    id=object_id,
                                    name=name_,
                                )
                            except ClientResponseError as e:
                                tries += 1
                                if verbose:
                                    logger.info(f"Client Response Error {str(text)}")
                                time.sleep((random.randint(0, 1000) / 1000) + 2**tries)
                                if tries > 2:
                                    if verbose:
                                        logger.error(f"retry failed in get_object function. \
                                                     Exiting with error status: {str(e)}")
                                    return DrsObject(
                                        self_uri=object_id,
                                        id=object_id,
                                        checksums=[],
                                        size=0,
                                        name=None,
                                        errors=[str(e)],
                                    )
                except ClientConnectorError as e:
                    tries += 1
                    if verbose:
                        logger.info(f"ClientConnectorError: {str(e)} while fetching object information")
                    time.sleep((random.randint(0, 1000) / 1000) + 2**tries)
                    if tries > 2:
                        if verbose:
                            logger.error(f"value of text error {str(text)}")
                            logger.error(f"retry failed in get_object function. Exiting with error status: {str(e)}")
                        return DrsObject(
                            self_uri=object_id,
                            id=object_id,
                            checksums=[],
                            size=0,
                            name=None,
                            errors=[str(e)],
                        )
