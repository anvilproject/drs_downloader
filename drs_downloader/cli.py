# flake8: noqa
import click
import async_downloader.download as async_downloader
from pathlib import Path
import os
from DRSClient import DRSClient
import pandas as pd
import aiohttp
import asyncio
import logging 
import subprocess
from tqdm import tqdm
import time
"""
logging.basicConfig()
logging.getLogger().setLevel(logging.DEBUG)
requests_log = logging.getLogger("requests.packages.urllib3")
requests_log.setLevel(logging.DEBUG)
requests_log.propagate = True
"""
    
def seperate_conflicts(numbers,urls,md5s_and_sizes):
    #add duplicate indexes to new array
    name_problems = []
    for i in range(len(numbers)):                
        name_problems. append([urls[numbers[i]],
        md5s_and_sizes[numbers[i]][0],md5s_and_sizes[numbers[i]][1]])

    for index in sorted(numbers, reverse=True):
        del urls[index]
        del md5s_and_sizes[index]
    

    return name_problems, urls, md5s_and_sizes

def duplicate_names(names):
    l1= []
    numbers = []
    names_2 = []
    count =0 
    for i in names:
        if i not in l1:
            l1.append(i)
        else:  
            names_2.append(i[0])
            numbers.append(count)
        count= count +1
    
    if(len(names_2)> 0):
        return names_2,numbers
    else:
        return False

def Extract_TSV_Information(Tsv_Path,duplicateflag,batchsize):
    urls = []
    md5s_and_sizes = []
    names = [] 
    df = pd.read_csv(Tsv_Path,sep = '\t')
    
    max_files = subprocess.check_output(["ulimit" ,"-n"]).decode("ascii")[0:-1]

    if(int(max_files) < int(batchsize)):
       raise Exception("The batch size that you have chosen is ",batchsize,\
            "your system only allows batchsizes of", max_files , " change the --batchsize flag to less than ",max_files,"to resolve this issue.")

    if('pfb:ga4gh_drs_uri' in df.columns.values.tolist()):
        for i in range(df['pfb:ga4gh_drs_uri'].count()):
            urls.append(df['pfb:ga4gh_drs_uri'][i])
            md5s_and_sizes.append([df['pfb:file_md5sum'][i],df['pfb:file_size'][i]])
            names.append([df['pfb:file_name'][i]])
    elif('ga4gh_drs_uri' in df.columns.values.tolist()):
        for i in range(df['ga4gh_drs_uri'].count()):
            urls.append(df['ga4gh_drs_uri'][i])
            md5s_and_sizes.append([df['file_md5sum'][i],df['file_size'][i]])
            names.append([df['file_name'][i]])
    else:
        raise KeyError("Key format is bad do either pfb:ga4gh_drs_uri or ga4gh_drs_uri")

    
    if (duplicate_names(names) != False and (duplicateflag=="FOLDER" or duplicateflag=="NAME")):
        name_problems, numbers =duplicate_names(names)

    elif(duplicateflag == "NONE" and duplicate_names(names) != False):
        name_problems, numbers =duplicate_names(names)
        raise Exception("Files sharing the same name have been found and duplicate flag has not been specified.  \
         The Files that are effected are: ", set(name_problems), " Reconfigure 'DUPLICATE' flag to 'NAME' or 'FILE' to resolve this error")
    else:
        name_problems= False
        numbers= False

    return urls,md5s_and_sizes,numbers

async def get_more(session,uri,url_endpoint,verbose):
    
    data = '{ "url": "'+uri+'", "fields": ["fileName", "size", "hashes", "accessUrl"]}'
    async with session.post(url=url_endpoint,data=data) as response:
        resp = await response.json(content_type=None) # content_type=None
        
        if( resp == None):
            return
        if (resp['fileName'] == None):
            print("A NONE TYPE HAS BEEN UNCOVERED")
            return 
        if (resp['accessUrl'] == None):
            print("A NONE TYPE URL HAS BEEN PASSED")
            return
        if (verbose == "YES"):
            print(resp['fileName']," has been successfully signed")
        return resp['accessUrl']['url']

async def get_signed_uris(uris,verbose,simulsign):
    #For people trying to Cut in line
    if(int(simulsign) > 10):
        simulsign= 10

    conn = aiohttp.TCPConnector(limit=simulsign)
    session_timeout =   aiohttp.ClientTimeout(total=1500)
    url_endpoint = "https://us-central1-broad-dsde-prod.cloudfunctions.net/martha_v3"
    token = subprocess.check_output(["gcloud" ,"auth" ,"print-access-token"]).decode("ascii")[0:-1]
    header = {'authorization': 'Bearer '+token, 'content-type': 'application/json'}
    async with aiohttp.ClientSession(trust_env = True ,headers=header ,connector= conn,timeout=session_timeout) as session:
        ret = await asyncio.gather(*[get_more(session,uri,url_endpoint,verbose) for uri in uris])
        return ret 


    #duplicateflag="FILE"
    #tsvname= 'smol.tsv'
    #outputfile = "/Users/peterkor/Desktop/terra-implementation/DATA"
    #Download_Files(tsvname,outputfile,duplicateflag)

@click.command()
@click.option('--duplicateflag', default="NONE", show_default=True, help='add --duplicateflag "NAME" to rename duplicates like untitiled_file(1) or --duplicateflag "FOLDER" \
to put every file downloaded into their own folder')
@click.option('--tsvname', default=None, show_default=True, help='The neame of the TSV file that you are downloading files from in quotations for example --tsvname "example.tsv" ')
@click.option('--outputfile', default=None, show_default=True, help='The file path of the output file to download to. Relative or Absolute')
@click.option('--batchsize', default=250, show_default=True, help='The number of files to download in a batch. For example --batchsize 500')
@click.option('--verbose', default=None, show_default=True, help='Displays every successful URL signing and Download happening in real time. Use --verbose "Yes" to display it')
@click.option('--simulsign', default=10, show_default=True, help='The amount of URLs to be signed at the same time. For example --simulsign 10 is the maxmimum for this argument')
#@click.option('--simuldown', default=None, show_default=True, help='The number of files to be downloaded at the same time. For example --simuldown None is the maxmimum for this argument')
def Download_Files(duplicateflag,tsvname,outputfile,batchsize,verbose,simulsign):
    drs_ids, md5s_and_sizes,numbers= Extract_TSV_Information(tsvname,duplicateflag,batchsize)
    print("Signing URIS")
    urls = asyncio.run(get_signed_uris(drs_ids,verbose,simulsign))

    if (numbers != False and (duplicateflag=="FOLDER" or duplicateflag=="NAME")):
        name_problems_catalogued,urls,md5s_and_sizes = seperate_conflicts(numbers,urls,md5s_and_sizes)

    print("Downloading URLS")
    download_obj = []
    name_problem_download_obj= [] 

    for url in urls:
        if url == None:
            continue
        download_url_bundle = async_downloader.DownloadURL(url,md5s_and_sizes[urls.index(url)][0],md5s_and_sizes[urls.index(url)][1])
        download_obj.append(download_url_bundle)

    if(numbers != False):
        print("YESSIR IN BAD FILE GROUPING")
        for name_problems in name_problems_catalogued:
            download_url_bundle = async_downloader.DownloadURL(name_problems[0],name_problems[1],name_problems[2])
            name_problem_download_obj.append(download_url_bundle)

    if(not Path(str(outputfile)).exists() and not os.path.exists(outputfile)):
        print("The path that you specified did not exist so the directory was made in your current working directory")
        os.mkdir(str(outputfile))

    if(duplicateflag != "FOLDER"):
        print("YESSIR DOWNLOAD NORMALLY")
        batches = 0
        while(batches<len(download_obj)):
            if(batches+batchsize>len(download_obj)):
                end = -1
            else:
                end = batches+batchsize
            async_downloader.download(download_obj[batches:end], Path(str(outputfile)),duplicateflag,verbose)
            batches=batches+batchsize

        
    #if there are more than 3000 duplicates this could easily become a problem since batching is not happening here yet.
    if(numbers != False and duplicateflag == "NAME"):
        print("YESSIR DOWNLOAD GHETTO")
        async_downloader.download(name_problem_download_obj, Path(str(outputfile)),duplicateflag,verbose)
        return

    elif(numbers != False and duplicateflag  == "FOLDER"):
        async_downloader.download(name_problem_download_obj, Path(str(outputfile)),duplicateflag,verbose)
        return


if __name__ == '__main__':
    Download_Files()
 
    


