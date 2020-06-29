# -*- coding: utf-8 -*-
"""
OPUSAPI class
"""

from functools import wraps
import json
import pandas as pd
import requests
import warnings

_DEFAULT_OPUS_SERVER = 'https://opus.pds-rings.seti.org'
_DEFAULT_FIELDS = ['opusid']

def hide_paging(data_name):
    """Automatically retrieve pages from OPUS and yield them in one stream."""
    # TODOAPI: The fact that we need to have a "data_name" here is an
    # inconsistency in the API.
    def _hide_paging(method):
        @wraps(method)
        def _impl(self, query=None, startobs=1, limit=None,
                  paging_limit=100, **method_kwargs):
            if startobs < 1:
                raise ValueError
            if limit is not None and limit < 1:
                raise ValueError
            if paging_limit is None:
                paging_limit = 100
            count = 0
            while limit is None or count < limit:
                ret = method(self, query, startobs, paging_limit,
                             **method_kwargs)
                data = ret[data_name]
                returned_count = ret['count']
                # TODOAPI: The fact that we return both dicts and lists
                # for different calls is an inconsistency in the API
                if isinstance(data, dict):
                    # For files.json and images.json
                    for opusid, fields in data.items():
                        yield {opusid: fields}
                        count += 1
                        if limit is not None and count >= limit:
                            break
                else:
                    # For data.json
                    for datum in data:
                        yield datum
                        count += 1
                        if limit is not None and count >= limit:
                            break
                available = ret['available']
                startobs += returned_count
                if startobs > available:
                    break
        return _impl
    return _hide_paging

class OPUSAPIRaw(object):
    """OPUSAPIRaw is an interface to the OPUS API that returns unprocessed,
       raw results from API calls. It is generally not recommended to use
       such a low level in application programs, but instead to use classes
       that build on the raw results to provide a nicer interface.
    """
    def __init__(self, server=None, default_fields=None, verbose=False):
        """Constructor for the OPUSAPIRaw class.

        :param server: If specified, will override the OPUS API server to talk
            to (defaults to opus.pds-rings.seti.org).
        :param default_fields: If specified, will override the default metadata
            fields to return if none of specified in future method calls
            (defaults to ['opusid']).
        :param verbose: If specified, provides verbose debugging output.
        """
        self._verbose = verbose

        if server is None:
            server = _DEFAULT_OPUS_SERVER
        else:
            if server.endswith('/'):
                server = server[:-1]
            if not server.startswith('http'):
                server = 'https://' + server
        self._server = server

        self._default_fields = (_DEFAULT_FIELDS if default_fields is None
                                                else default_fields)
        self._raw_fields_cache = None
        self._raw_fields_as_df_cache = None

    def __str__(self):
        return self._server

    def __repr__(self):
        return 'OPUSAPIRaw for server '+self._server

    def _call_opus_api(self, endpoint, return_format, params={}):
        """Make a call to the OPUS sever for a specific endpoint."""
        request_url = self._server+'/api/'+endpoint+'.'+return_format
        if self._verbose:
            print(f'OPUSAPI request {request_url} params {params}')
        r = requests.get(request_url, params=params)
        if not r.ok:
            raise RuntimeError(f'OPUSAPI request failed: {request_url} ' +
                               f' with params {params}')
        return r.json()

    @property
    def raw_fields(self):
        """Return the raw set of OPUS fields as a dict indexed by fieldid."""
        if self._raw_fields_cache is not None:
            return self._raw_fields_cache

        fields_json = self._call_opus_api('fields', 'json')
        fields_ret = fields_json['data']

        # Get rid of unnecessary fields that are present for backwards
        # compatibility
        for raw_fieldid in fields_ret:
            if 'slug' in fields_ret[raw_fieldid]:
                del fields_ret[raw_fieldid]['slug']
            if 'old_slug' in fields_ret[raw_fieldid]:
                del fields_ret[raw_fieldid]['old_slug']

        self._raw_fields_cache = fields_ret
        return self._raw_fields_cache

    @property
    def raw_fields_as_df(self):
        """Return the raw set of OPUS fields as a DataFrame indexed by fieldid."""
        if self._raw_fields_as_df_cache is not None:
            return self._raw_fields_as_df_cache

        raw_fields = self.raw_fields
        raw_fieldids = raw_fields.keys() # Get keys once to guarantee ordering
        categories = [raw_fields[id]['category'] for id in raw_fieldids]
        types = [raw_fields[id]['type'] for id in raw_fieldids]
        labels = [raw_fields[id]['label'] for id in raw_fieldids]
        full_labels = [raw_fields[id]['full_label'] for id in raw_fieldids]
        search_labels = [raw_fields[id]['search_label'] for id in raw_fieldids]
        full_search_labels = [raw_fields[id]['full_search_label']
                              for id in raw_fieldids]
        default_units = [raw_fields[id]['default_units'] for id in raw_fieldids]
        available_units = [raw_fields[id]['available_units']
                           for id in raw_fieldids]

        ret_frame = pd.DataFrame({'category': categories,
                                  'type': types,
                                  'label': labels,
                                  'full_label': full_labels,
                                  'search_label': search_labels,
                                  'full_search_label': full_search_labels,
                                  'default_units': default_units,
                                  'available_units': available_units},
                                 index=raw_fieldids)

        self._raw_fields_as_df_cache = ret_frame
        return self._raw_fields_as_df_cache

    ### Meta API Calls

    def get_count_raw(self, query=None):
        """Return the raw result count from a search."""
        params = None if query is None else query.get_api_params(opusapi=self)
        res = self._call_opus_api('meta/result_count', 'json', params=params)
        return res['data']

    def get_mults_raw(self, fieldid, query=None):
        """Return the available values from a multiple choice field along with
        their result count from a search."""
        params = None if query is None else query.get_api_params(opusapi=self)
        res = self._call_opus_api('meta/mults/'+fieldid, 'json', params=params)
        return res['mults']

    def get_range_endpoints_raw(self, fieldid, query=None):
        """Return the endpoints for a range based on a search."""
        params = None if query is None else query.get_api_params(opusapi=self)
        res = self._call_opus_api('meta/range/endpoints/'+fieldid, 'json',
                                  params=params)
        return res

    ### Metadata, Files, Images API Calls

    @property
    def default_fields(self):
        return self._default_fields

    def _normalize_fields(self, fields):
        if fields is None:
            fields = self.default_fields
        if isinstance(fields, str):
            fields = fields.split(',')
        raw_fields = self.raw_fields
        for field in fields:
            if field not in raw_fields:
                raise RuntimeError(f'Unknown field id "{field}"')
        return ','.join(fields)

    def _normalize_product_types(self, product_types):
        if product_types is None:
            return None
        # TODO: Read and store available product types like we do for fields
        # and then validate them here
        return ','.join(product_types)

    @hide_paging('page')
    def get_metadata_raw(self, query, startobs, limit, fields=None):
        """Return the results of raw calls to data.json.

        This returns a list. Each list element is a list of metadata
        corresponding to the requested fields. All fields are returned
        as strings regardless of the underlying field type.

        Example:
            [['co-iss-n1454939333', '2004-02-08T13:25:41.089', '18'],
             ['co-iss-n1454939373', '2004-02-08T13:26:36.496', '2.6']]
        """
        params = {} if query is None else query.get_api_params(opusapi=self)
        params['startobs'] = startobs
        params['limit'] = limit
        params['cols'] = self._normalize_fields(fields)
        res = self._call_opus_api('data', 'json', params=params)
        return res

    @hide_paging('data')
    def get_files_raw(self, query, startobs, limit, product_types=None):
        """Return the results of raw calls to files.json.

        This returns a list. Each list element is a dict where the key is
        the OPUS ID and the value is a dict with keys as product types and
        values as URLs.

        Example:
            [{'co-iss-n1454725799': {
                [...]
                'coiss_calib': [
                    'https://pds-rings.seti.org/ ... N1454725799_1_CALIB.IMG',
                    'https://pds-rings.seti.org/ ... N1454725799_1_CALIB.LBL'],
                [...]
             }]
        """
        params = {} if query is None else query.get_api_params(opusapi=self)
        params['startobs'] = startobs
        params['limit'] = limit
        types = self._normalize_product_types(product_types)
        if types is not None:
            params['types'] = types
        res = self._call_opus_api('files', 'json', params=params)
        return res

    @hide_paging('data')
    def get_images_raw(self, query, startobs, limit, size=None):
        """Return the results of raw calls to images.json.

        This returns a list. Each list element is a dict where the key is
        the image field and the value is the associated value.

        Example:
            If a size is not specified:

            [{'full_alt_text': 'N1454725799_1_small.jpg',
              'full_height': 256,
              [...]
              'full_url': 'https://pds-rings.seti.org/ ... N1454725799_1_small.jpg',
              'full_width': 256}]

            If a size is specified:

            [{'alt_text': 'N1454725799_1_small.jpg',
              'height': 256,
              [...]
              'url': 'https://pds-rings.seti.org/ ... N1454725799_1_small.jpg',
              'width': 256}]
        """
        params = {} if query is None else query.get_api_params(opusapi=self)
        params['startobs'] = startobs
        params['limit'] = limit
        image_url = 'images'
        if size is not None:
            size = size.lower()
            assert size in (None, 'thumb', 'small', 'med', 'full')
            image_url += '/'+size
        res = self._call_opus_api(image_url, 'json', params=params)
        return res
