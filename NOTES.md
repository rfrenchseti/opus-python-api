Existing packages:

* https://github.com/michaelaye/pyciss/blob/master/pyciss/opusapi.py
* https://github.com/seignovert/python-opus-seti

Ideas from brainstorming between Rob French and Michael Aye:

* Use the urlpath package
* Use pandas to return data
* When an OPUS ID is provided as output, make it a clickable link in
iPython/Jupiter that takes you to the OPUS Detail tab
* Abstract away the concept of paging - perhaps use a generator
* Provide pretty-printing of fields
* Add docstrings (tooltips) to fields.json
* Use astropy tables to return metadata and allow automatic unit conversion
