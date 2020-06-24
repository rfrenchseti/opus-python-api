import json
import pandas as pd
import requests

class OPUSAPI(object):
    OPUS_SERVER = 'https://opus.pds-rings.seti.org'

    def __init__(self, server=None):
        if server is None:
            server = self.OPUS_SERVER
        self._server = server
        self._raw_fields_cache = None
        pass

    def _call_opus_api(self, endpoint, return_format, params={}):
        """Make a call to the OPUS sever for a specific endpoint."""
        r = requests.get(self._server+'/api/'+endpoint+'.'+return_format,
                         params=params)
        return r.json()

    def get_raw_fields(self):
        """Return the raw set of OPUS fields as a dict indexed by fieldid."""
        if self._raw_fields_cache is not None:
            return self._raw_fields_cache

        fields_json = self._call_opus_api('fields', 'json')
        fields_ret = fields_json['data']

        for raw_fieldid in fields_ret:
            if 'slug' in fields_ret[raw_fieldid]:
                del fields_ret[raw_fieldid]['slug']
            if 'old_slug' in fields_ret[raw_fieldid]:
                del fields_ret[raw_fieldid]['old_slug']

        self._raw_fields_cache = fields_ret

        return self._raw_fields_cache

    def get_raw_fields_as_df(self):
        """Return the raw set of OPUS fields as a DataFrame indexed by fieldid."""
        raw_fields = self.get_raw_fields()
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

        return pd.DataFrame({'category': categories,
                             'type': types,
                             'label': labels,
                             'full_label': full_labels,
                             'search_label': search_labels,
                             'full_search_label': full_search_labels,
                             'default_units': default_units,
                             'available_units': available_units},
                            index=raw_fieldids)

    def _get_fields(self):
        """Return the analyzed set of OPUS fields as a dict indexed by fieldid."""
        raw_fields = self.get_raw_fields()

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

    def get_fields(self):
        """Return the analyzed set of OPUS fields as a dict indexed by fieldid."""
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
                   'default_units': default_units[i],
                   'available_units': available_units[i]
                 } for i in range(len(fieldid_roots))
              }

        return ret

    def get_fields_as_df(self):
        """Return the analyzed set of OPUS fields as a DataFrame indexed by fieldid."""
        fields = self.get_fields()
        fieldids = fields.keys()

        categories = [fields[id]['category'] for id in fieldids]
        types = [fields[id]['type'] for id in fieldids]
        fieldid1s = [fields[id]['fieldid1'] for id in fieldids]
        fieldid2s = [fields[id]['fieldid2'] for id in fieldids]
        label1s = [fields[id]['label1'] for id in fieldids]
        label2s = [fields[id]['label2'] for id in fieldids]
        full_label1s = [fields[id]['full_label1'] for id in fieldids]
        full_label2s = [fields[id]['full_label2'] for id in fieldids]
        search_labels = [fields[id]['search_label'] for id in fieldids]
        full_search_labels = [fields[id]['full_search_label']
                              for id in fieldids]
        default_units = [fields[id]['default_units'] for id in fieldids]
        available_units = [fields[id]['available_units']
                           for id in fieldids]

        return pd.DataFrame({'category': categories,
                             'type': types,
                             'fieldid1': fieldid1s,
                             'fieldid2': fieldid2s,
                             'label1': label1s,
                             'label2': label2s,
                             'full_label1': full_label1s,
                             'full_label2': full_label2s,
                             'search_label': search_labels,
                             'full_search_label': full_search_labels,
                             'default_units': default_units,
                             'available_units': available_units},
                            index=fieldids)
