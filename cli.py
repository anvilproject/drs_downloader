import click
import requests
import json
import async_downloader.download as async_downloader
from pathlib import Path
import os.path
import os
from DRSClient import DRSClient
import pandas as pd
import aiohttp
import asyncio
import time
import logging 
import subprocess

"""
logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True
"""



def _send_request(tsv_name):
    urls = [] 
    drs_ids, md5s_and_sizes= Extract_TSV_Information(tsv_name)
    url = "https://us-central1-broad-dsde-prod.cloudfunctions.net/martha_v3"

    for id in drs_ids:
        print(id)
        token = subprocess.check_output(["gcloud" ,"auth" ,"print-access-token"]).decode("ascii")[0:-1]
        header = {'authorization': 'Bearer '+token, 'content-type': 'application/json'}
        data = '{ "url": "'+id+'", "fields": ["fileName", "size", "hashes", "accessUrl"]}'
        response = requests.post(url=url,headers=header,data=data)
        json_resp = json.loads(response.content.decode('utf-8'))['accessUrl']['url']
        download_url_bundle = async_downloader.DownloadURL(json_resp,md5s_and_sizes[drs_ids.index(id)][0],md5s_and_sizes[drs_ids.index(id)][1])
        urls.append(download_url_bundle)
    
    return urls

    
def Extract_TSV_Information(Tsv_Path):
    urls = []
    md5s_and_sizes = []
    df = pd.read_csv(Tsv_Path,sep = '\t')

    for i in range(df['pfb:ga4gh_drs_uri'].count()):
        urls.append(df['pfb:ga4gh_drs_uri'][i])
        md5s_and_sizes.append([df['pfb:file_md5sum'][i],df['pfb:file_size'][i]])
    return urls,md5s_and_sizes


if __name__ == '__main__':
    tsv_name= 'smol.tsv'
    urls = _send_request(tsv_name)
    async_downloader.download(urls, Path('DATA'))


