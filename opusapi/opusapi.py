# -*- coding: utf-8 -*-
"""
OPUSAPI class
"""

from functools import wraps
import json
import pandas as pd
import requests
import warnings

from .util import CaseInsensitiveDict
from .opusapiraw import OPUSAPIRaw

class OPUSAPI(OPUSAPIRaw):
    def __init__(self, server=None, default_fields=None, verbose=False):
        """Constructor for the OPUSAPI class."""
        super(OPUSAPI, self).__init__(server=server,
                                      default_fields=default_fields,
                                      verbose=verbose)
        self._fields_cache = None
        self._fields_as_df_cache = None
        self._surfacegeo_targets_cache = None
        self._surfacegeo_fields_cache = None

    def __repr__(self):
        return 'OPUSAPI for server '+self._server

    def _get_fields(self):
        """Analyze the fields to figure out single- and two-valued ranges."""
        if self._fields_cache is not None:
            return self._fields_cache

        raw_fields = self.raw_fields

        collapsed_fields = {}

        # Note: We relay on the fact that the OPUS fields.json API returns
        # fields in a certain order ('1' before '2').
        for raw_fieldid, raw_field in raw_fields.items():
            if raw_fieldid[-1] in ('1', '2'):
                raw_fieldid = raw_fieldid[:-1]
            if raw_fieldid not in collapsed_fields:
                collapsed_fields[raw_fieldid] = []
            collapsed_fields[raw_fieldid].append(raw_field)

        fieldid_roots = []
        categories = []
        types = []
        label1s = []
        full_label1s = []
        label2s = []
        full_label2s = []
        search_labels = []
        full_search_labels = []
        default_units = []
        available_units = []

        for fieldid_root, raw_fields in collapsed_fields.items():
            category = raw_fields[0]['category']
            f_type = raw_fields[0]['type']
            raw_field1 = raw_fields[0]
            raw_field2 = None
            if len(raw_fields) == 2:
                raw_field2 = raw_fields[1]

            search_label = raw_field1['search_label']
            full_search_label = raw_field1['full_search_label']

            label1 = raw_field1['label']
            full_label1 = raw_field1['full_label']
            fieldid1 = fieldid_root

            label2 = None
            full_label2 = None
            fieldid2 = None
            if len(raw_fields) == 2:
                fieldid1 = fieldid_root + '1'
                fieldid2 = fieldid_root + '2'
                label2 = raw_field2['label']
                full_label2 = raw_field2['full_label']

            # Note: Units will always be the same for '1' and '2'
            default_unit = raw_field1['default_units']
            available_unit = raw_field1['available_units']

            categories.append(category)
            fieldid_roots.append(fieldid_root)
            types.append(f_type)
            label1s.append(label1)
            label2s.append(label2)
            full_label1s.append(full_label1)
            full_label2s.append(full_label2)
            search_labels.append(search_label)
            full_search_labels.append(full_search_label)
            default_units.append(default_unit)
            available_units.append(available_unit)

        return (fieldid_roots, categories, types,
                label1s, label2s,
                full_label1s, full_label2s,
                search_labels, full_search_labels,
                default_units, available_units)

    @property
    def fields(self):
        """Return the analyzed set of OPUS fields as a dict indexed by fieldid."""
        if self._fields_cache is not None:
            return self._fields_cache

        (fieldid_roots, categories, types, label1s, label2s,
         full_label1s, full_label2s, search_labels, full_search_labels,
         default_units, available_units) = self._get_fields()

        ret = {fieldid_roots[i]: {
                   'category': categories[i],
                   'fieldid1': fieldid_roots[i] if label2s[i] is None
                                                else fieldid_roots[i]+'1',
                   'label1': label1s[i],
                   'full_label1': full_label1s[i],
                   'fieldid2': None if label2s[i] is None
                                    else fieldid_roots[i]+'2',
                   'label2': label2s[i],
                   'full_label2': full_label2s[i],

                   'search_fieldid1':
                        fieldid_roots[i]+'1' if types[i].startswith('range')
                                             else fieldid_roots[i],
                   'search_fieldid2':
                        fieldid_roots[i]+'2' if types[i].startswith('range')
                                             else None,
                   'search_label': search_labels[i],
                   'full_search_label': full_search_labels[i],
                   'type': types[i],
                   'single_value': label2s[i] is None,
                   'default_units': default_units[i],
                   'available_units': available_units[i]
                 } for i in range(len(fieldid_roots))
              }

        self._fields_cache = ret
        return self._fields_cache

    def _extract_fields_as_df(self, fields):
        """Convert fields into a DataFrame."""
        fieldids = fields.keys()

        categories = [fields[id]['category'] for id in fieldids]
        types = [fields[id]['type'] for id in fieldids]
        fieldid1s = [fields[id]['fieldid1'] for id in fieldids]
        fieldid2s = [fields[id]['fieldid2'] for id in fieldids]
        label1s = [fields[id]['label1'] for id in fieldids]
        label2s = [fields[id]['label2'] for id in fieldids]
        full_label1s = [fields[id]['full_label1'] for id in fieldids]
        full_label2s = [fields[id]['full_label2'] for id in fieldids]
        search_fieldid1s = [fields[id]['search_fieldid1'] for id in fieldids]
        search_fieldid2s = [fields[id]['search_fieldid2'] for id in fieldids]
        search_labels = [fields[id]['search_label'] for id in fieldids]
        full_search_labels = [fields[id]['full_search_label']
                              for id in fieldids]
        single_values = [fields[id]['single_value'] for id in fieldids]
        default_units = [fields[id]['default_units'] for id in fieldids]
        available_units = [fields[id]['available_units']
                           for id in fieldids]

        ret_frame = pd.DataFrame({'category': categories,
                                  'type': types,
                                  'fieldid1': fieldid1s,
                                  'fieldid2': fieldid2s,
                                  'label1': label1s,
                                  'label2': label2s,
                                  'full_label1': full_label1s,
                                  'full_label2': full_label2s,
                                  'search_fieldid1': search_fieldid1s,
                                  'search_fieldid2': search_fieldid2s,
                                  'search_label': search_labels,
                                  'full_search_label': full_search_labels,
                                  'single_value': single_values,
                                  'default_units': default_units,
                                  'available_units': available_units},
                                 index=fieldids)
        return ret_frame

    @property
    def fields_as_df(self):
        """Return the analyzed set of OPUS fields as a DataFrame indexed by fieldid."""
        if self._fields_as_df_cache is not None:
            return self._fields_as_df_cache

        ret_frame = self._extract_fields_as_df(self.fields)

        self._fields_as_df_cache = ret_frame
        return self._fields_as_df_cache

    def _extract_surfacegeo_targets_fields(self):
        """Extract surfacegeo targets and fields."""
        if self._surfacegeo_targets_cache is not None:
            return (self._surfacegeo_targets_cache,
                    self._surfacegeo_fields_cache)

        raw_fields = self.fields

        target_dict = CaseInsensitiveDict()
        fields_dict = {}
        for fieldid, field in raw_fields.items():
            if not fieldid.startswith('SURFACEGEO'):
                continue
            field_split = fieldid[10:].split('_')
            if len(field_split) != 2:
                warnings.warn('Bad format for surface geometry field: '+fieldid)
                continue
            label = field['full_search_label']
            if '[' not in label or ']' not in label:
                warnings.warn('Bad format for surface geometry label: '+label)
                continue
            target_name = label[label.index('[')+1:label.index(']')]
            target_dict[target_name] = field_split[0]
            if field_split[1] not in fields_dict:
                fields_dict[field_split[1]] = field

        self._surfacegeo_targets_cache = target_dict
        self._surfacegeo_fields_cache = fields_dict

        return (self._surfacegeo_targets_cache,
                self._surfacegeo_fields_cache)

    @property
    def surfacegeo_targets(self):
        """Return the list of targets that surface geometry is available for."""
        return self._extract_surfacegeo_targets_fields()[0]

    @property
    def surfacegeo_fields(self):
        """Return the available surface geometry metadata fields."""
        return self._extract_surfacegeo_targets_fields()[1]

    @property
    def surfacegeo_fields_as_df(self):
        """Return the available surface geometry metadata fields."""
        fields = self._extract_surfacegeo_targets_fields()[1]
        return self._extract_fields_as_df(fields)

    def make_surfacegeo_field(self, target, field_root):
        """Construct a fieldid from a target name and fieldid root."""
        targets = self.surfacegeo_targets
        # Look up the pretty name in case it was provided
        if target in targets:
            target = targets[target]
        target = target.lower()
        return f'SURFACEGEO{target}_{field_root}'

    ### Meta API Calls

    def get_count(self, query=None):
        """Return the result count from a search."""
        res = self.get_count_raw(query)
        return int(res[0]['result_count'])

    def get_mults(self, fieldid, query=None):
        """Return the available values from a multiple choice field along with
        their result count from a search."""
        if fieldid not in self.fields:
            raise RuntimeError(f'Field id "{fieldid}" unknown')
        if self.fields[fieldid]['type'] != 'multiple':
            raise RuntimeError(f'Field id "{fieldid}" is not type "multiple"')
        return self.get_mults_raw(fieldid, query=query)

    def get_range_endpoints(self, fieldid, query=None):
        """Return the endpoints for a range based on a search."""
        params = None if query is None else query.get_api_params(opusapi=self)
        if fieldid not in self.fields:
            raise RuntimeError(f'Field id "{fieldid}" unknown')
        if not self.fields[fieldid]['type'].startswith('range'):
            raise RuntimeError(f'Field id "{fieldid}" is not type "range"')
        res = self.get_range_endpoints_raw(fieldid, query=query)
        return res['min'], res['max'], res['nulls'], res['units']

    ### Metadata, Files, Images API Calls

    def get_metadata(self, query=None, startobs=1, limit=None,
                     paging_limit=None, fields=None):
        """Return the results of calls to data.json.

        TODO XXX
        This returns a list. Each list element is a list of metadata
        corresponding to the requested fields. All fields are returned
        as strings regardless of the underlying field type.

        Example:
            [['co-iss-n1454939333', '2004-02-08T13:25:41.089', '18'],
             ['co-iss-n1454939373', '2004-02-08T13:26:36.496', '2.6']]
        """
        return self.get_metadata_raw(query=query, startobs=startobs,
                                     limit=limit, fields=fields)

    def get_files(self, query=None, startobs=1, limit=None,
                  paging_limit=None, product_types=None):
        """Return the results of raw calls to files.json.

        TODO XXX
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
        return self.get_files_raw(query=query, startobs=startobs,
                                  limit=limit, product_types=product_types)

    def get_images(self, query=None, startobs=1, limit=None,
                   paging_limit=None, size=None):
        """Return the results of raw calls to images.json.

        TODO XXX
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
        return self.get_images_raw(query=query, startobs=startobs,
                                   limit=limit, size=size)
