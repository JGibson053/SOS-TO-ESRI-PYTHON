# SOS-TO-ESRI-PYTHON
Python code for scraping data from Hilltop OGC SOS services to ESRI geodatabase
Python 2.7 script for ESRI ArcGIS Server 10.3.1
Extracts values from OGC SOS services provided from a Hilltop web server.
Updates values into a ESRI FGDB geodatabase feature class, which is used as the basis for ESRI REST webservices.
Uses Python xml.etree.ElementTree and ArcPy
Example SOS webservice : http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Hutt%20River%20at%20Kaitoke&ObservedProperty=Stage
Example ESRI REST service : http://mapping.gw.govt.nz/arcgis/rest/services/GW/River_flows_P/MapServer
See also : http://mapping.gw.govt.nz/News08_River_Flows_and_Rainfall.htm

Initial commit to Master 20 May 2018
