#!/usr/bin/env python3
import json
import os
import logging
from pathlib import Path

import click
import fastavro
import jwt
import requests
import urllib.parse

from gen3.auth import Gen3Auth
from gen3.file import Gen3File
from gen3.index import Gen3Index

from gen3.submission import Gen3Submission

from cdislogging import get_logger as get_gen3_logger
import base64

log_fmt = "%(asctime)s %(name)s %(levelname)s : %(message)s"

# set logging to warning, since gen3.submission logs a verbose INFO message on each call :-()
logging.basicConfig(level=logging.WARNING, format=log_fmt)
# set gen3's logger as well
get_gen3_logger('__name__', log_level='warn', format=log_fmt)


def get_logger_(name):
    """Return logger with level set to info"""
    logger = logging.getLogger(name)
    logger.setLevel(logging.INFO)
    return logger


def extract_endpoint(gen3_credentials_file):
    """Get base url of jwt issuer claim."""
    with open(gen3_credentials_file) as input_stream:
        api_key = json.load(input_stream)['api_key']
        claims = jwt.decode(api_key, options={"verify_signature": False})
        assert 'iss' in claims
        return claims['iss'].replace('/user', '')


@click.group()
@click.option('--gen3_credentials_file', default='credentials.json', show_default=True,
              help='API credentials file downloaded from gen3 profile.')
@click.pass_context
def cli(ctx, gen3_credentials_file):
    """File uploader."""

    endpoint = extract_endpoint(gen3_credentials_file)
    get_logger_("cli").info(endpoint)
    get_logger_("cli").debug(f"Read {gen3_credentials_file} endpoint {endpoint}")
    auth = Gen3Auth(endpoint, refresh_file=gen3_credentials_file)
    ctx.ensure_object(dict)
    ctx.obj['submission_client'] = Gen3Submission(endpoint, auth)
    ctx.obj['file_client'] = Gen3File(endpoint, auth)
    ctx.obj['index_client'] = Gen3Index(endpoint, auth)

    ctx.obj['endpoint'] = endpoint
    ctx.obj['programs'] = [link.split('/')[-1] for link in ctx.obj['submission_client'].get_programs()['links']]


def file_attributes(file_name):
    """Calculate the hash and size."""
    import hashlib

    md5_hash = hashlib.md5()

    with open(file_name, "rb") as f:
        # Read and update hash in chunks of 4K
        for byte_block in iter(lambda: f.read(4096), b""):
            md5_hash.update(byte_block)

    return md5_hash.hexdigest(), os.lstat(file_name).st_size


@cli.command()
@click.option('--did', default=None, show_default=True,
              help='GUID from indexd')
@click.pass_context
def get_index(ctx, did):
    """Read index."""
    assert did, "Missing did (guid) parameter"
    result = ctx.obj['index_client'].get_record(did)
    print("record", result)
    result = ctx.obj['file_client'].get_presigned_url(did)
    print("presigned_url", result)



@cli.command()
@click.option('--bucket_name', default='aced-default', show_default=True,
              help='File name to upload')
@click.option('--pfb_path', required=True, default=None, show_default=True,
              help='Path to pfb file')
@click.option('--program', default='MyFirstProgram', show_default=True,
              help='Gen3 program')
@click.option('--project', default='MyFirstProject', show_default=True,
              help='Gen3 project')
@click.pass_context
def upload_pfb(ctx, bucket_name, pfb_path, program, project):
    """Filter DocumentReference records found in PFB to gen3 managed bucket, update hashes and size."""

    index_client = ctx.obj['index_client']
    file_client = ctx.obj['file_client']

    """Finds DocumentReferences with attachment urls."""
    for record in pfb_reader(pfb_path):
        if record['name'] == 'DocumentReference':
            if record['object']['content_attachment_url']:
                object_name = record['object']['content_attachment_url'].lstrip('./')
                content_attachment_md5 = record['object']['content_attachment_md5']
                content_attachment_size = record['object']['content_attachment_size']
                record_id = record['id']
                # print(record['id'], object_name, content_attachment_md5, content_attachment_size)
                assert 'name' in record, record.keys()
                record_name = record['name']
                assert 'submitter_id' in record['object'], record.keys()
                submitter_id = record['object']['submitter_id']
                document = file_client.upload_file(object_name, bucket=bucket_name)
                assert 'guid' in document, document
                assert 'url' in document, document
                signed_url = urllib.parse.unquote(document['url'])
                guid = document['guid']
                hashes = {'md5': content_attachment_md5}
                metadata = {
                    **{
                        'datanode_type': record_name,
                        'datanode_submitter_id': submitter_id,
                        'datanode_object_id': guid
                    },
                    **hashes}

                # open the file and upload it to the signed url returned above
                with open(object_name, 'rb') as data:
                    # When you use this header, Amazon S3 checks the object against the provided MD5 value and,
                    # if they do not match, returns an error.
                    content_md5 = base64.b64encode(bytes.fromhex(content_attachment_md5))
                    headers = {'Content-MD5': content_md5}
                    # Our meta data
                    for key, value in metadata.items():
                        headers[f"x-amz-meta-{key}"] = value
                    r = requests.put(signed_url, data=data, headers=headers)
                    assert r.status_code == 200, (signed_url, r.text)
                    get_logger_("upload").info(f"Successfully uploaded file \"{object_name}\" to GUID {guid}")

                # update the indexd record with urls, authz, size and hashes
                indexd_record = index_client.get_record(guid)
                assert 'rev' in indexd_record, record
                rev = indexd_record['rev']
                r = index_client.update_blank(guid, rev, hashes=hashes, size=content_attachment_size)

                urls = [f"s3://{bucket_name}/{guid}/{object_name}"]
                authz = [f'/programs/{program}/projects/{project}']

                result = index_client.update_record(guid=guid, version=rev, file_name=object_name,
                                                    authz=authz, urls=urls,
                                                    metadata=metadata)
                assert result   # TODO - better check
                print(json.dumps({'id': record_id, 'name': record_name, 'object_id': guid}))


@cli.command()
@click.option('--bucket_name', default='aced-default', show_default=True,
              help='File name to upload')
@click.option('--project_path', required=True, default=None, show_default=True,
              help='Path to simulated metadata data see: shorturl.at/jqvZ5')
@click.option('--pfb_path', required=False, default=None, show_default=True,
              help='Path to pfb file')
@click.option('--program', default='MyFirstProgram', show_default=True,
              help='Gen3 program')
@click.option('--project', default='MyFirstProject', show_default=True,
              help='Gen3 project')
@click.pass_context
def upload(ctx, bucket_name, project_path, pfb_path, program, project):
    """Filter data_files found in generated synthetic data to gen3 managed bucket, update hashes and size."""

    index_client = ctx.obj['index_client']
    file_client = ctx.obj['file_client']

    # this code reads files created by gen3's test meta data
    # https://github.com/uc-cdis/compose-services/blob/master/docs/using_the_commons.md#generating-test-metadata
    project_path = Path(project_path)
    for synthetic_data_path in list(project_path.glob('**/*.json')):
        updated_records = []
        for record in json.load(open(synthetic_data_path, "r")):
            if 'file_name' not in record:
                break

            object_name = record['file_name'].lstrip('/')

            # create a record in gen3, get a signed url
            document = file_client.upload_file(object_name, bucket=bucket_name)
            assert 'guid' in document, document
            assert 'url' in document, document
            signed_url = urllib.parse.unquote(document['url'])
            guid = document['guid']
            hashes = {'md5': record['md5sum']}
            metadata = {
                **{
                    'datanode_type': record['type'],
                    'datanode_submitter_id': record['submitter_id'],
                    'datanode_object_id': guid
                },
                **hashes}

            # open the file and upload it to the signed url returned above
            with open(record['file_name'], 'rb') as data:
                # When you use this header, Amazon S3 checks the object against the provided MD5 value and,
                # if they do not match, returns an error.
                content_md5 = base64.b64encode(bytes.fromhex(record['md5sum']))
                headers = {'Content-MD5': content_md5}
                # Our meta data
                for key, value in metadata.items():
                    headers[f"x-amz-meta-{key}"] = value
                r = requests.put(signed_url, data=data, headers=headers)
                assert r.status_code == 200, (signed_url, r.text)
                get_logger_("upload").info(f"Successfully uploaded file \"{record['file_name']}\" to GUID {guid}")

            # update the indexd record with urls, authz, size and hashes
            indexd_record = index_client.get_record(guid)
            assert 'rev' in indexd_record, record
            rev = indexd_record['rev']
            r = index_client.update_blank(guid, rev, hashes=hashes, size=record["file_size"])

            urls = [f"s3://{bucket_name}/{guid}/{object_name}"]
            authz = [f'/programs/{program}/projects/{project}']

            result = index_client.update_record(guid=guid, version=rev, file_name=record['file_name'],
                                                authz=authz, urls=urls,
                                                metadata=metadata)
            assert result   # TODO - better check

            record['object_id'] = guid
            updated_records.append(record)

        if len(updated_records) > 0:
            with open(synthetic_data_path, "w") as fp:
                json.dump(updated_records, fp, indent=4)
                print(f"Uploaded data and update object_id in records in {synthetic_data_path}")


def pfb_reader(pfb_path):
    """Yields records from pfb."""
    reader = fastavro.read.reader
    with open(pfb_path, 'rb') as fo:
        for record in reader(fo):
            yield record


@cli.command()
@click.option('--did', default=None, show_default=True,
              help='GUID from indexd')
@click.option('--file_name', default=None, show_default=True,
              help='output path')
@click.pass_context
def drs_download(ctx, did, file_name):
    """
    https://github.com/ga4gh/fasp-clients/blob/55dad8373637765bae43a1c670afc5f2b7b302b8/src/fasp/loc/gen3drsclient.py#L60
    """
    assert did, "Missing did (guid) parameter"
    result = ctx.obj['index_client'].get_record(did)

    assert 'hashes' in result, f'Expected "hashes" {result}'
    assert 'md5' in result['hashes'], f'Expected "hashes.md5" {result}'
    assert 'size' in result, f'Expected "size" {result}'

    md5 = result['hashes']['md5']
    size = result['size']
    result = ctx.obj['file_client'].get_presigned_url(did)
    assert 'url' in result, f'Expected "url" {result}'
    presigned_url = result['url']
    from download import download, DownloadURL
    download_url = DownloadURL(url=presigned_url, md5=md5, size=size)
    download(urls=[download_url])

#
# @cli.command()
# @click.option('--did', default=None, show_default=True,
#               help='GUID from indexd')
# @click.option('--file_name', default=None, show_default=True,
#               help='output path')
# @click.pass_context
# def download(ctx, did, file_name):
#     """Download file_name."""
#     assert did, "Missing did (guid) parameter"
#     result = ctx.obj['file_client'].get_presigned_url(did)
#     assert 'url' in result, result
#     presigned_url = result['url']
#     if not file_name:
#         result = ctx.obj['index_client'].get_record(did)
#         file_name = result['file_name']
#
#     # NOTE the stream=True parameter below
#     # see https://stackoverflow.com/questions/16694907/download-large-file-in-python-with-requests
#     with requests.get(presigned_url, allow_redirects=True, stream=True) as r:
#         r.raise_for_status()
#         with open(file_name, 'wb') as f:
#             for chunk in r.iter_content(chunk_size=8192):
#                 # If you have chunk encoded response uncomment if
#                 # and set chunk_size parameter to None.
#                 #if chunk:
#                 f.write(chunk)


if __name__ == '__main__':
    cli()
