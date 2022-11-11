import requests
import json
import logging


class DRSClient:
    def __init__(self, api_url_base, access_id=None, public=False, debug=False):
        '''Initialize a DRS Client for the service at the specified url base
        api_url_base
        access_id - the default access id to use when obtaining a URL for a  given object id
        public - an indicator that the data to be accessed through this client is public, and that authentication
                 is not required
        debug - whether debug level informstion should be printed
        '''
        self.api_url_base = api_url_base
        self.access_id = access_id
        self.debug = None  # debug
        self.public = public
        self.authorized = False
        self.access_token = None

    def getHeaders(self):
        return {'Authorization': 'Bearer {0}'.format(self.access_token)}

    def get_access_url(self, object_id, access_id=None):
        if access_id is None:
            access_id = self.access_id

        if not self.public:
            headers = self.getHeaders()
        else:
            headers = {}

        api_url = '{0}/access/{1}'.format(object_id, access_id)
        if self.debug:
            logger.debug(api_url)

        response = requests.get(api_url, headers=headers)
        if self.debug:
            print(response)
        if response.status_code == 200:
            resp = response.content.decode('utf-8')
            return json.loads(resp)['url']
        if response.status_code == 401:
            print('Unauthorized for that DRS id')
            return None
        else:
            print(response)
            print(response.content)
            return None
