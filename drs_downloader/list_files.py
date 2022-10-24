from gen3 import *


auth = Gen3Auth(refresh_file="credentials.json")
index = Gen3Index(auth)



def get_index(ctx, did):
    """Read index."""
    assert did, "Missing did (guid) parameter"
    result = ctx.obj['index_client'].get_record(did)
    print("record", result)
    result = ctx.obj['file_client'].get_presigned_url(did)
    print("presigned_url", result)