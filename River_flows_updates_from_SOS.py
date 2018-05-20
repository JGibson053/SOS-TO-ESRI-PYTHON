# River_flows.py
# 
# Request water level data from Hilltop using SOS services & parse XML to get required values
# Get stage (water level) and flow (computed by Hilltop) for all sites in collection of live monitored sites
# Populates a feature class with latest values
# Using this as standard call to services with hts will drop intermittent sites from output
# Here make separate call for each sitename from a list which seems more reliable
# 
# Written JRG 09 May 2017
# Modified - 19-JUN-2017 JRG Added error traps
#            23-JUN-2017 JRG Modified rise/fall calcs & threshold value
#            07-SEP-2017 JRG Calculate value for new [FlowSymbol] field

import arcpy
import requests
import sys, os, string, datetime, math
import xml.etree.ElementTree as ET
from xml.etree.ElementTree import ParseError

# Create list of URLs for "Stage" measurements, loop through them & build up results arrays
url_list = []

url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Kopuaranga%20at%20Palmers%20Bridge&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Hutt%20River%20at%20Kaitoke&ObservedProperty=Stage')
url_list.append("http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Waiohine%20River%20at%20Gorge&ObservedProperty=Stage")
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Akatarawa%20River%20at%20Cemetery&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Horokiri%20Stream%20at%20Snodgrass&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Huangarua%20at%20Hautotara&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Hutt%20River%20at%20Birchville&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Hutt%20River%20at%20Estuary%20Bridge&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Hutt%20River%20at%20Kaitoke&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Mangaone%20Stream%20at%20Ratanui&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Mangaroa%20River%20at%20Te%20Marua&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Mangatarere%20River%20at%20Gorge&ObservedProperty=Stage')
# No data for Stage [Water Level : 0] at Mill Creek at Papanui
#url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Mill%20Creek%20at%20Papanui&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Otaki%20River%20at%20Pukehinau&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Pauatahanui%20Stream%20at%20Gorge&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Porirua%20Stream%20at%20Town%20Centre&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Ruamahanga%20River%20at%20Gladstone%20Bridge&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Ruamahanga%20River%20at%20Mt%20Bruce&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Ruamahanga%20River%20at%20Waihenga%20Bridge&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Ruamahanga%20River%20at%20Wardells&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Taueru%20River%20at%20Te%20Weraiti&ObservedProperty=Stage')
#url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Taupo%20Stream%20at%20Flax%20Swamp&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Waikanae%20River%20at%20Water%20Treatment%20Plant&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Waingawa%20River%20at%20Kaituna&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Wainuiomata%20River%20at%20Leonard%20Wood%20Park&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Wainuiomata%20River%20at%20Manuka%20Track&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Waiohine%20River%20at%20Gorge&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Waipoua%20River%20at%20Mikimiki%20Bridge&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Waitohu%20Stream%20at%20Water%20Supply%20Intake&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Waiwhetu%20Stream%20at%20Whites%20Line%20East&ObservedProperty=Stage')
# Whangaehu reading seems to be frozen at 2016
#url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Whangaehu%20River%20at%20Waihi&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Wharemauku%20Stream%20at%20Coastlands&ObservedProperty=Stage')
# Additions 4 May 2017
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Horokiri%20Stream%20at%20Snodgrass&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Booths%20Creek%20at%20Andersons%20Line&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Fernridge%20Water%20Supply&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Floodway%20at%20Hikinui%20Sill&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Floodway%20at%20Jenkins%20Dip&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Floodway%20at%20Oporua&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Hutt%20River%20at%20Taita%20Gorge&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Hutt%20River%20at%20Te%20Marua&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Kopuaranga%20River%20at%20Stuarts&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Lake%20Kohangapiripiri&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Lake%20Kohangatera&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Lake%20Onoke%20at%20Lake%20Ferry&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Lake%20Wairarapa%20at%20Barrage%20North&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Lake%20Wairarapa%20at%20Burlings&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Lake%20Wairarapa%20Barrage%20Argonaut&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Mangatarere%20at%20Belvedere%20Bridge&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Mangatarere%20River%20at%20State%20Highway%202&ObservedProperty=Stage')
#url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Mill%20Creek%20at%20Papanui&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Orongorongo%20River%20at%20Truss%20Bridge&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Otakura%20Stream%20at%20Upstream%20JKD%20Weir&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Otakura%20Stream%20at%20Weir&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Pahaoa%20River%20at%20Hinakura&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Papawai%20Stream%20at%20Fabians%20Road&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Tauherenikau%20at%20Gorge&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Taumata%20Oxbow%20Lagoon&ObservedProperty=Stage')
#url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Taupo%20Stream%20at%20Flax%20Swamp&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Te%20Hapua%20Wetland%20at%20Pateke&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Te%20Hapua%20Wetland%20at%20Shoveler%20Lagoon&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Te%20Mara%20Stream%20at%20Kiriwhakapapa&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Tent%20Lagoon&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Ruamahanga%20River%20at%20Barrage%20South&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Burlings%20Stream%20at%20Western%20Lake%20Road&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Lake%20Onoke%20at%20Lake%20Ferry&ObservedProperty=Stage')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Taueru%20River%20at%20Te%20Whiti%20Rd%20Bridge&ObservedProperty=Stage')

#======================================================================================================================

# This is the summary log file of service results
infolderroot = "D:\\Temp\\River_flows"
logfile = infolderroot + "\\SOS_River_Flow_service_logfile.txt" 
logfile = open(logfile, "w+")
#logfile = open(logfile, "a")
logfile.write("Time Started " + str(datetime.datetime.now()) + "\n")
logfile.write("===================================\n")

# Declare empty dictionaries for data readings
LatestStage = {}
LatestTime = {}
StageUnits = {}

for url in url_list:
    
    logfile.write("\n")

    try :
        response = requests.get(url)    # can print response.status_code  # e.g. 200
    except :
        logfile.write("ERROR : Get error from " + url + " at " + str(datetime.datetime.now()) + "\n")

    if (response.status_code == requests.codes.ok) :
        
        # Parse returned XML using ElementTree
        try:
            root = ET.fromstring(response.text)
        except ParseError as err:
            logfile.write("ERROR : Parse error in XML with code " + str(err.code))
        except:
            logfile.write("ERROR : Parsing response from " + url + " at " + str(datetime.datetime.now()) + "\n")

        # Get feature name within Result tag
        myfeatureOfInterest = root.find('{http://www.opengis.net/waterml/2.0}observationMember/{http://www.opengis.net/om/2.0}OM_Observation/{http://www.opengis.net/om/2.0}featureOfInterest')

        if myfeatureOfInterest is not None:
        
            sitename = myfeatureOfInterest.attrib.get('{http://www.w3.org/1999/xlink}title')
            logfile.write(sitename + " ")
            
            myMTS = root.find('{http://www.opengis.net/waterml/2.0}observationMember/{http://www.opengis.net/om/2.0}OM_Observation/{http://www.opengis.net/om/2.0}result/{http://www.opengis.net/waterml/2.0}MeasurementTimeseries')
            myUom = myMTS.find('{http://www.opengis.net/waterml/2.0}defaultPointMetadata/{http://www.opengis.net/waterml/2.0}DefaultTVPMeasurementMetadata/{http://www.opengis.net/waterml/2.0}uom')
            units = myUom.attrib.get('code')

            myMTVP = myMTS.find('{http://www.opengis.net/waterml/2.0}point/{http://www.opengis.net/waterml/2.0}MeasurementTVP')

            # Get the observation time & value
            myValue = myMTVP.findtext("{http://www.opengis.net/waterml/2.0}value")
            # Record the response response.text
            logfile.write(myValue + " ")

            myTime = myMTVP.findtext("{http://www.opengis.net/waterml/2.0}time") # 2017-05-03T17:00:00+12:00
            # Record the response response.text
            logfile.write(myTime[:19])
            logfile.write("\n")

            LatestStage[sitename] = myValue
            LatestTime[sitename] = myTime[:19]  # Date/Time (3) - "2016-01-22T10:00:00"
            StageUnits[sitename] = units

        else :

            # Record warning if required elements not found in response XML (typically ExceptionReport)
            logfile.write("WARNING : no observation data from " + url + " at " + str(datetime.datetime.now()) + "\n")

    else :

        # Record the response response.text if get fails
        logfile.write("ERROR : Code " + response.status_code + " from " + url + " at " + str(datetime.datetime.now()) + "\n")

# Now do flow values
url_list = []
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Kopuaranga%20at%20Palmers%20Bridge&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Hutt%20River%20at%20Kaitoke&ObservedProperty=Flow')
url_list.append("http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Waiohine%20River%20at%20Gorge&ObservedProperty=Flow")
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Akatarawa%20River%20at%20Cemetery&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Horokiri%20Stream%20at%20Snodgrass&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Huangarua%20at%20Hautotara&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Hutt%20River%20at%20Birchville&ObservedProperty=Flow')
#url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Hutt%20River%20at%20Estuary%20Bridge&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Hutt%20River%20at%20Kaitoke&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Mangaone%20Stream%20at%20Ratanui&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Mangaroa%20River%20at%20Te%20Marua&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Mangatarere%20River%20at%20Gorge&ObservedProperty=Flow')
# No data for Stage [Water Level : 0] at Mill Creek at Papanui
#url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Mill%20Creek%20at%20Papanui&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Otaki%20River%20at%20Pukehinau&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Pauatahanui%20Stream%20at%20Gorge&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Porirua%20Stream%20at%20Town%20Centre&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Ruamahanga%20River%20at%20Gladstone%20Bridge&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Ruamahanga%20River%20at%20Mt%20Bruce&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Ruamahanga%20River%20at%20Waihenga%20Bridge&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Ruamahanga%20River%20at%20Wardells&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Taueru%20River%20at%20Te%20Weraiti&ObservedProperty=Flow')
#url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Taupo%20Stream%20at%20Flax%20Swamp&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Waikanae%20River%20at%20Water%20Treatment%20Plant&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Waingawa%20River%20at%20Kaituna&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Wainuiomata%20River%20at%20Leonard%20Wood%20Park&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Wainuiomata%20River%20at%20Manuka%20Track&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Waiohine%20River%20at%20Gorge&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Waipoua%20River%20at%20Mikimiki%20Bridge&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Waitohu%20Stream%20at%20Water%20Supply%20Intake&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Waiwhetu%20Stream%20at%20Whites%20Line%20East&ObservedProperty=Flow')
# Whangaehu reading seems to be frozen at 2016
#url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Whangaehu%20River%20at%20Waihi&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Wharemauku%20Stream%20at%20Coastlands&ObservedProperty=Flow')
# Additions 4 May 2017
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Horokiri%20Stream%20at%20Snodgrass&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Booths%20Creek%20at%20Andersons%20Line&ObservedProperty=Flow')
#url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Fernridge%20Water%20Supply&ObservedProperty=Flow')
#url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Floodway%20at%20Hikinui%20Sill&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Floodway%20at%20Jenkins%20Dip&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Floodway%20at%20Oporua&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Hutt%20River%20at%20Taita%20Gorge&ObservedProperty=Flow')
#url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Hutt%20River%20at%20Te%20Marua&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Kopuaranga%20River%20at%20Stuarts&ObservedProperty=Flow')
#url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Lake%20Kohangapiripiri&ObservedProperty=Flow')
#url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Lake%20Kohangatera&ObservedProperty=Flow')
#url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Lake%20Onoke%20at%20Lake%20Ferry&ObservedProperty=Flow')
#url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Lake%20Wairarapa%20at%20Barrage%20North&ObservedProperty=Flow')
#url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Lake%20Wairarapa%20at%20Burlings&ObservedProperty=Flow')
#url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Lake%20Wairarapa%20Barrage%20Argonaut&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Mangatarere%20at%20Belvedere%20Bridge&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Mangatarere%20River%20at%20State%20Highway%202&ObservedProperty=Flow')
#url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Mill%20Creek%20at%20Papanui&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Orongorongo%20River%20at%20Truss%20Bridge&ObservedProperty=Flow')
#url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Otakura%20Stream%20at%20Upstream%20JKD%20Weir&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Otakura%20Stream%20at%20Weir&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Pahaoa%20River%20at%20Hinakura&ObservedProperty=Flow')
#url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Papawai%20Stream%20at%20Fabians%20Road&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Tauherenikau%20at%20Gorge&ObservedProperty=Flow')
#url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Taumata%20Oxbow%20Lagoon&ObservedProperty=Flow')
#url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Taupo%20Stream%20at%20Flax%20Swamp&ObservedProperty=Flow')
#url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Te%20Hapua%20Wetland%20at%20Pateke&ObservedProperty=Flow')
#url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Te%20Hapua%20Wetland%20at%20Shoveler%20Lagoon&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Te%20Mara%20Stream%20at%20Kiriwhakapapa&ObservedProperty=Flow')
#url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Tent%20Lagoon&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Burlings%20Stream%20at%20Western%20Lake%20Road&ObservedProperty=Flow')
url_list.append('http://hilltop.gw.govt.nz/Data.hts?Service=SOS&Request=GetObservation&FeatureOfInterest=Taueru%20River%20at%20Te%20Whiti%20Rd%20Bridge&ObservedProperty=Flow')

LatestFlow = {}
FlowUnits = {}

for url in url_list:
    
    logfile.write("\n")

    try :
        response = requests.get(url)    # can print response.status_code  # e.g. 200
    except :
        logfile.write("ERROR : Get error from " + url + " at " + str(datetime.datetime.now()) + "\n")

    if (response.status_code == requests.codes.ok):

        try:
            root = ET.fromstring(response.text)
        except ParseError as err:
            logfile.write("ERROR : Parse error in XML with code " + str(err.code))
        except:
            logfile.write("ERROR : Parsing response from " + url + " at " + str(datetime.datetime.now()) + "\n")

        # Get feature name within Result tag
        myfeatureOfInterest = root.find('{http://www.opengis.net/waterml/2.0}observationMember/{http://www.opengis.net/om/2.0}OM_Observation/{http://www.opengis.net/om/2.0}featureOfInterest')

        if myfeatureOfInterest is not None:
            
            sitename = myfeatureOfInterest.attrib.get('{http://www.w3.org/1999/xlink}title')
            logfile.write(sitename + " ") # Site name as string

            myObservation = root.find('{http://www.opengis.net/waterml/2.0}observationMember/{http://www.opengis.net/om/2.0}OM_Observation')
            
            myMTS = root.find('{http://www.opengis.net/waterml/2.0}observationMember/{http://www.opengis.net/om/2.0}OM_Observation/{http://www.opengis.net/om/2.0}result/{http://www.opengis.net/waterml/2.0}MeasurementTimeseries')

            myUom = myMTS.find('{http://www.opengis.net/waterml/2.0}defaultPointMetadata/{http://www.opengis.net/waterml/2.0}DefaultTVPMeasurementMetadata/{http://www.opengis.net/waterml/2.0}uom')
            units = myUom.attrib.get('code')    # e.g. "m3/sec"

            myMTVP = myMTS.find('{http://www.opengis.net/waterml/2.0}point/{http://www.opengis.net/waterml/2.0}MeasurementTVP')

            # Get the observation time & value
            myValue = myMTVP.findtext("{http://www.opengis.net/waterml/2.0}value")
            logfile.write(myValue + " ")

            LatestFlow[sitename] = myValue
            FlowUnits[sitename] = units

        else :

            # Record warning if required elements not found in response XML (typically ExceptionReport)
            logfile.write("WARNING : no observation data from " + url + " at " + str(datetime.datetime.now()) + "\n")
    else :

        # Record the response response.text if get fails
        logfile.write("ERROR : Code " + response.status_code + " from " + url + " at " + str(datetime.datetime.now()) + "\n")

# ================================================================================================

# Update geodatabase table based off values from webservice response into the dictionaries
myTab = "D:/Data/River_flows/Flow_data.gdb/All_Gauged_sites_20170505"

# Create a update cursor. Could use a faster DA cursor from ArcGIS 10.1 onwards
#
rows = arcpy.UpdateCursor(myTab) 

# Run a cursor through all rows in the geodatabase table, update field values from dictionaries
for row in rows:

    # Geodatabase table should already contain all valid site names as a key. Site name syntax must match.
    mySiteName = row.getValue("Name")

    # Temp fix to insert value, just disable URL call later
    if mySiteName == "Ruamahanga River at Gladstone Bridge" :
        LatestFlow[mySiteName] = 0.0
        FlowUnits[mySiteName] = "N/A"

    try:
        stageLevel = float(LatestStage[mySiteName])  # This will give a key error if site not in data returned by webservice
    except KeyError:
        continue # Skip this site

    try:
        flowLevel = round(float(LatestFlow[mySiteName]),2)  # This will give a key error if data not returned by webservice
    except KeyError:
        flowLevel = 0.0 # Flow may not be recorded, e.g. Hutt River at Estuary, as it is tidal.

    try:
        flowUnits = FlowUnits[mySiteName]  # This will give a key error if data not returned by webservice
    except KeyError:
        flowUnits = "N/A" # Flow units may not be recorded, e.g. Hutt River at Estuary, as it is tidal.
    
    
    # Copy previous values before update
    myPrevValue = row.getValue("LatestStage")
    row.setValue("PreviousStage",myPrevValue)
    myPrevTime = row.getValue("LatestTime")
    row.setValue("PreviousTime",myPrevTime)

    # Update fields in current record from matching dictionary entries
    row.setValue("LatestStage",stageLevel)
    row.setValue("LatestFlow",flowLevel)
    row.setValue("LatestTime",LatestTime[mySiteName])
    row.setValue("StageUnits",StageUnits[mySiteName])
    row.setValue("FlowUnits",flowUnits)

    myMinValue = row.getValue("MinStage")
    myMaxValue = row.getValue("MaxStage")

    # Update field [Change] in current record for Rising/Steady/Falling
    if stageLevel > myPrevValue :
        changeState = "Rising"
        row.setValue("Change","Rising")
    else :
        changeState = "Falling"
        row.setValue("Change","Falling")

    # n.b. value 0.01 below is sensitive to time interval between refreshes of script (currently every 30 mins in PRDE)
    myDifference = abs(stageLevel - myPrevValue)
    aboveMinStage = (stageLevel - myMinValue)
    if aboveMinStage == 0 :
        aboveMinStage = 1
    if myDifference / aboveMinStage < 0.01 :
        changeState = "Steady"
        row.setValue("Change","Steady")

    # Update field [Stage_pct] for current stage as percentage of last years stage range
    currentPercentage = ((stageLevel - myMinValue)/(myMaxValue - myMinValue)) * 100.0
    row.setValue("Stage_pct",currentPercentage)

    #logfile.write("Write time : " + LatestTime[mySiteName] + "\n")

    # Update field [FlowSymbol] in current record.
    # This will be a hidden field in the service, which defines the symbology
    if currentPercentage < 20.0 :
        flowSymbol = "%s_low" % (changeState) 
    elif currentPercentage < 50.0 :
        flowSymbol = "%s_med" % (changeState) 
    elif currentPercentage < 80.0 :
        flowSymbol = "%s_high" % (changeState) 
    else:
        flowSymbol = "%s_vhigh" % (changeState) 
    row.setValue("FlowSymbol",flowSymbol)

    rows.updateRow(row)
    
del row,rows

# =============== End of main program ====================================
logfile.write("\n")
logfile.write("Time Finished " + str(datetime.datetime.now()) + "\n")
logfile.close()

print "=== Done ==="





