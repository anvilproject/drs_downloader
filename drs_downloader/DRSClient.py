import requests
import json
import re
#from builtins import None

class DRSClient:
	def __init__(self, api_url_base, access_id=None, public=False, debug=False):
		'''Initialize a DRS Client for the service at the specified url base
		api_url_base
		access_id  the default access id to use when obtaining a URL for a  given object id
		public an indicator that the data to be accessed through this client is public, adn that suthentication is not required
	 -boolean
	 debug - whether debug level informstion should be printed
		'''
		self.api_url_base = api_url_base
		self.access_id = access_id
		self.id = None
		self.name = None
		self.version = None
		self.debug = debug
		self.public = public
		self.authorized = False


    # Get info about a DrsObject
    # See https://ga4gh.github.io/data-repository-service-schemas/preview/develop/docs/#_get_object
	def get_object(self, object_id, expand=False):
		''' Implementation of the DRS getObject method
		object_id
		expand - whether or not bundles should be expanded - boolean 
		'''
		api_url = '{0}/ga4gh/drs/v1/objects/{1}'.format(self.api_url_base, object_id)
		if expand:
			api_url += '?expand=true'
		if self.debug:
			print(api_url)
		# headers generated error on SRA, doesn't seem to be required by the others
		#headers = {'Content-Type': 'application/json'}
		#response = requests.get(api_url, headers=headers)
		response = requests.get(api_url)
		response.raise_for_status()
		resp = response.content.decode('utf-8')

		return json.loads(resp)
		#=======================================================================
		# if response.status_code == 200:
		# 	resp = response.content.decode('utf-8')
		# 	return json.loads(resp)
		# else:
		# 	print(response.content.decode('utf-8'))
		# 	return response.status_code
		#=======================================================================

	# Get a URL for fetching bytes. 
	# See https://ga4gh.github.io/data-repository-service-schemas/preview/develop/docs/#_get_access_url
	def get_access_url(self, object_id, access_id=None):
		''' Implementation of the DRS get URL to access bytes method
		object_id
		access_id a valid  access id for this object_id on the specified DRS server
		by default the access id supplied for the client will be used
		'''
		if access_id == None:
			access_id = self.access_id
		
		if not self.public:
			headers = self.getHeaders()
		else:
			headers ={}
		#headers['Content-Type'] = 'application/json'
		api_url = '{0}/ga4gh/drs/v1/objects/{1}/access/{2}'.format(self.api_url_base, object_id, access_id)
		if self.debug:
			print(api_url)
		response = requests.get(api_url, headers=headers)
		if self.debug: print(response)
		if response.status_code == 200:
			resp = response.content.decode('utf-8')
			return json.loads(resp)['url']
		if response.status_code == 401:
			print('Unauthorized for that DRS id')
			return None
		else:
			print (response)
			print (response.content)
			return None

	def getHeaders(self):
		return {'Authorization' : 'Bearer {0}'.format(self.access_token) }