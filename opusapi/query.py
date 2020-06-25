# -*- coding: utf-8 -*-
"""
OPUS Query class
"""

class Query(object):
    """Construct a conjunctive (AND) series of queries."""
    def __init__(self, *args):
        """Query constructor."""
        self._conj_list = []
        for arg in args:
            self._conj_list.append(arg)

    def __str__(self):
        ret = 'OPUS Query with search terms:'
        for conj in self._conj_list:
            ret += '\n  ' + str(conj)
        return ret

    def __repr__(self):
        conj_repr = []
        for conj in self._conj_list:
            conj_repr.append(repr(conj))
        return 'Query(' + ','.join(conj_repr) + ')'

    def get_api_params(self, opusapi=None):
        """Get the OPUS API parameters required for a search."""
        params = {}
        for conj in self._conj_list:
            # This is an obscure but efficient way to make sure the same
            # fieldid doesn't appear twice and throw an exception if it does
            params = dict(**params, **conj.get_api_params(opusapi=opusapi))
        return params

class OR(object):
    """Construct a disjunctive (OR) series of queries."""
    def __init__(self, *args):
        self._disj_list = []
        fieldid = None
        for arg in args:
            if not isinstance(arg, (StringQuery, RangeQuery)):
                raise RuntimeError(
                    'Only StringQuery and RangeQuery may be used with OR')
            self._disj_list.append(arg)
            if fieldid is None:
                fieldid = arg.fieldid
            elif fieldid != arg.fieldid:
                raise RuntimeError(
                    'Attempt to create OR query with different fieldids ' +
                    f'"{fieldid}" and "{arg.fieldid}"')

    def __str__(self):
        ret = 'OR:'
        for disj in self._disj_list:
            ret += '\n    ' + str(disj)
        return ret

    def __repr__(self):
        disj_repr = []
        for disj in self._disj_list:
            disj_repr.append(repr(disj))
        return 'OR(' + ','.join(disj_repr) + ')'

    def get_api_params(self, opusapi=None):
        """Get the OPUS API parameters required for a search."""
        params = {}
        for idx, disj in enumerate(self._disj_list):
            # This is an obscure but efficient way to make sure the same
            # fieldid doesn't appear twice and throw an exception if it does
            params = dict(**params, **disj.get_api_params(opusapi=opusapi,
                                                          suffix=idx+1))
        return params

class MultQuery(Query):
    def __init__(self, fieldid, vals):
        super(MultQuery, self).__init__()
        if not isinstance(vals, (tuple, list)):
            if ',' in vals:
                vals = vals.split(',')
            else:
                vals = [vals]
        self._fieldid = fieldid
        self._vals = vals

    def __str__(self):
        ret = f'MultQuery {self._fieldid}=' + ','.join(self._vals)
        return ret

    def __repr__(self):
        if len(self._vals) != 1:
            val_repr = repr(self._vals)
        else:
            val_repr = repr(self._vals[0])
        return f'MultQuery({repr(self._fieldid)},{val_repr})'

    @property
    def fieldid(self):
        return self._fieldid

    def get_api_params(self, opusapi=None):
        """Get the OPUS API parameters required for a search."""
        if opusapi is not None:
            fields = opusapi.fields
            if self._fieldid not in fields:
                raise RuntimeError(f'Unknown field id "{self._fieldid}"')
            f_type = fields[self._fieldid]['type']
            if f_type != 'multiple':
                raise RuntimeError(f'Field id "{self._fieldid}" is type ' +
                                   f'"{f_type}" not type "multiple"')

        return {self._fieldid: ','.join(self._vals)}

class StringQuery(Query):
    def __init__(self, fieldid, val, qtype='contains'):
        super(StringQuery, self).__init__()
        qtype = qtype.lower()
        assert qtype in ('contains', 'begins', 'ends', 'matches', 'excludes',
                         'regex')
        self._fieldid = fieldid
        self._val = val
        self._qtype = qtype

    def __str__(self):
        ret = f'StringQuery {self._fieldid}={self._val} ({self._qtype})'
        return ret

    def __repr__(self):
        return (f'StringQuery({repr(self._fieldid)},{repr(self._val)},' +
                f'{repr(self._qtype)})')

    @property
    def fieldid(self):
        return self._fieldid

    def get_api_params(self, opusapi=None, suffix=None):
        """Get the OPUS API parameters required for a search."""
        if opusapi is not None:
            fields = opusapi.fields
            if self._fieldid not in fields:
                raise RuntimeError('Unknown field id "'+self._fieldid+'"')
            f_type = fields[self._fieldid]['type']
            if f_type != 'string':
                raise RuntimeError(f'Field id "{self._fieldid}"' +
                                   f' is type "{f_type}" not type "string"')

        fieldid = self._fieldid
        if suffix is not None:
            fieldid += '_'+str(suffix)
        return {fieldid: self._val,
                'qtype-'+fieldid: self._qtype}

class RangeQuery(Query):
    def __init__(self, fieldid, minimum=None, maximum=None, qtype=None,
                 unit=None):
        super(RangeQuery, self).__init__()
        qtype = None if qtype is None else qtype.lower()
        # We don't enforce a default qtype because single-value range fields
        # don't take a qtype at all
        assert qtype in (None, 'any', 'all', 'only')
        self._fieldid = fieldid
        self._min = None if minimum is None else float(minimum)
        self._max = None if maximum is None else float(maximum)
        self._qtype = qtype
        self._unit = None if unit is None else unit.lower()

    def __str__(self):
        ret = 'RangeQuery '+self._fieldid
        if self._min:
            ret += f' min={self._min}'
        if self._max:
            ret += f' max={self._max}'
        if self._qtype is not None:
            ret += f' (qtype {self._qtype})'
        if self._unit is not None:
            ret += f' [{self._unit}]'
        return ret

    def __repr__(self):
        param_list = [repr(self._fieldid)]
        if self._min is not None:
            param_list.append('minimum='+repr(self._min))
        if self._max is not None:
            param_list.append('maximum='+repr(self._max))
        if self._qtype is not None:
            param_list.append('qtype='+repr(self._qtype))
        if self._unit is not None:
            param_list.append('unit='+repr(self._unit))
        return 'RangeQuery(' + ','.join(param_list) + ')'

    @property
    def fieldid(self):
        return self._fieldid

    def get_api_params(self, opusapi=None, suffix=None):
        """Get the OPUS API parameters required for a search."""
        if opusapi is not None:
            fields = opusapi.fields
            if self._fieldid not in fields:
                raise RuntimeError('Unknown field id "'+self._fieldid+'"')
            field = fields[self._fieldid]
            f_type = field['type']
            if not f_type.startswith('range'):
                raise RuntimeError(f'Field id "{self._fieldid}" is type ' +
                                   f'"{f_type}" not type "range"')
            if field['single_value'] and self._qtype is not None:
                raise RuntimeError(f'Field id "{self._fieldid}" ' +
                                   'is single value but qtype supplied')
            available_units = field['available_units']
            if (self._unit is not None and
                self._unit not in available_units):
                avail_str = ','.join(available_units)
                raise RuntimeError(f'Field id "{self._fieldid}" ' +
                                   f'unit "{self._unit}" is unknown ' +
                                   f'(available: {avail_str})')
        suffix_str = ''
        if suffix is not None:
            suffix_str = '_' + str(suffix)
        params = {}
        if self._min:
            params[self._fieldid+'1'+suffix_str] = self._min
        if self._max:
            params[self._fieldid+'2'+suffix_str] = self._max
        if self._min is not None or self._max is not None:
            if self._qtype is not None:
                params['qtype-'+self._fieldid+suffix_str] = self._qtype
            if self._unit is not None:
                params['unit-'+self._fieldid+suffix_str] = self._unit
        return params
