import argparse, boto3, sys, time
import numpy as np
import pandas as pd
import re
from botocore import UNSIGNED
from botocore.client import Config
from calendar import timegm
from datetime import date, datetime, timedelta
from glob import glob
from os import path, system
from shapely.geometry import polygon, Point
from tqdm import tqdm

def main(outputDir = "temp", dateFormat = "%Y%m%d-%H%M", startDates = [], endDates = [],
         inputFile = '', copyST = False, copyNRE = False, printFileList = False,
         radars = [], radarName = "radar", timeStampName = "timestamp",
         startTimeName = "startDate", endTimeName = "endDate", timeThreshold = 300,
         domains = [], radarFile = '', latName = '', lonName = '', radarCol = ''):   
   if not path.exists(outputDir):
      print("Output directory {} does not exist. Creating directory.".format(outputDir), flush = True)
      system("mkdir -p {}".format(outputDir))      
   
   if not "%Y" or not "%m" or not "%d" in dateFormat:
      print("\"{}\" is not a valid date format".format(dateFormat), flush = True)
      return 0

   if inputFile == '':
      if (len(radars) == 0) and (len(domains) != 0):
         if radarFile == '':
            print("No list of radars supplied. Please provide a CSV with radar lats and lons.", flush = True)
            exit()
         radars = getRadarListFromDomain(domains, radarFile, latName, lonName, radarCol)
         print("Radars in domain:")
         print(radars)
      if (len(startDates) == 0 ) or (len(endDates) == 0) or (len(radars) == 0):
         print("One of the range/radar variables is empty and no CSV is specified.", flush = True)
         return 0
      epochTime = useRangeAndRadar(startDates, endDates, radars, dateFormat)
   else:
      if not path.exists(inputFile):
         print("Input file \"{}\" does not exist.".format(inputFile), flush = True)
         return 0
   
      if timeThreshold < 0:
         print("Time threshold ({}) cannot be negative.".format(timeThreshold), flush = True)
         return 0
      epochTime = useCSV(inputFile,  radarName,  timeStampName, startTimeName, 
                         endTimeName, dateFormat, timeThreshold)
   
   # Make sure there is input data
   if len(epochTime) == 0:
      print("No valid data was found. Check that your date/radar format is valid.", flush = True)
      return 0
   
   # Split into more entries if start and end times are more than 1 day apart
   # Make list of prefixes to find the relevant objects from Amazon bucket 
   prefixList = np.array([[(datetime.utcfromtimestamp(item["start"]).date() +
                            timedelta(days = numDays)).strftime("%Y/%m/%d/") +
                           item["radar"] + '/'] for item in epochTime 
                           for numDays in range((datetime.utcfromtimestamp(item["end"]).date() -
                                                 datetime.utcfromtimestamp(item["start"]).date()).days + 1)])
   
   # Flatten and find unique entries in list
   prefixList = np.array(list(set([item for sublist in prefixList for item in sublist])))
      
   # Setup Anoymous Login for S3 with Amazon 
   noaas3 = boto3.client("s3", region_name = "us-east-1",
                         config = Config(signature_version = UNSIGNED))
   
   # Make list of files in the bucket for the dates specified
   print("Finding files to download:", flush = True)
   objectsInBucket = np.array([noaas3.list_objects_v2(Bucket = "noaa-nexrad-level2",
                                                      Delimiter = '/', Prefix = prefix)
                               for prefix in tqdm(prefixList, file = sys.__stdout__)])
   objectList = np.array([item["Contents"] for item in objectsInBucket if "Contents" in item], dtype=object)
   # Make sure files are of the right format
   fileNames = np.array([file["Key"] for object in objectList
                         for file in object
                         if "Key" in file and
                         (file["Key"][file["Key"].rfind('/') + 1:file["Key"].rfind('/') + 13] ==
                          file["Key"][file["Key"].rfind('/') - 4:file["Key"].rfind('/')] +
                          file["Key"][0:4] + file["Key"][5:7] + file["Key"][8:10]) and
                         (file["Key"][-3:] != "MDM") and (file["Key"][-4:] != ".001")])
   # Make dictionary with the file name, radar, and UNIX time for each file
   fileInfo = np.array([{"name" : name,
                         "radar" : name[name.rfind('/') + 1:name.rfind('/') + 5],
                         "time" : timegm(time.strptime(name[name.rfind('/') + 5:name.rfind('/') + 20],
                                                       "%Y%m%d_%H%M%S"))} for name in fileNames])
     
   radars = np.array([entry["radar"] for entry in fileInfo])
   
   if printFileList:
      print("Files:", flush = True)
      for file in fileInfo: print(file, flush = True)
   
   # Filtering out files outside of the time window
   print("Filtering files to download:", flush = True)
   filteredFiles = [info["name"] for epoch in tqdm(epochTime, file = sys.__stdout__)
                    for info in fileInfo[np.where(radars == epoch["radar"])[0]]
                    if (epoch["start"] <= info["time"] <= epoch["end"])] 
      
   filteredFiles.sort()
   
   filteredDates = list(set([file[file.rfind('/') + 5:file.rfind('/') + 13] + '/' + file[file.rfind('/') + 1:file.rfind('/') + 5] for file in filteredFiles]))
      
   # Make set of existing radar files in specified output directory for skipping downloading existing files
   print("Finding exisiting files:", flush = True)
   existingFiles = set([item[item.rfind('/') + 5:item.rfind('/') + 9] + '/' +
                        item[item.rfind('/') + 9:item.rfind('/') + 11] + '/' +
                        item[item.rfind('/') + 11:item.rfind('/') + 13] + '/' +
                        item[item.rfind('/') + 1:item.rfind('/') + 5] + '/' +
                        item[item.rfind('/') + 1:]
                        for d in tqdm(filteredDates, file = sys.__stdout__)
                        for item in glob(outputDir + "/{}/raw/*".format(d))])
        
   # Filtering out files that are already in output directory
   filesToDownload = np.array(list(set(filteredFiles) - existingFiles))
   
   filesToDownload.sort()
   
   # Return if no files need to be downloaded
   if len(filesToDownload) == 0:
      if inputFile:
         print("No new files to download for {}".format(inputFile), flush = True)
      else:
         print("No new files to download.", flush = True)
      return 0
   
   # Print list of files to download if this setting is set
   if printFileList:
      print("Files to download:", flush = True)
      for file in filesToDownload: print(file, flush = True)
      
   # Download files and make directories if they don't already exist
   print("Downloading files:", flush = True)
   for file in tqdm(filesToDownload, file = sys.__stdout__):
      if not path.exists(outputDir + '/' + file[file.rfind('/') + 5:file.rfind('/') + 13] +
                         file[file.rfind('/'):file.rfind('/') + 5] + "/raw"):
         system("mkdir -p {}/{}{}/raw".format(outputDir,file[file.rfind('/') + 5:file.rfind('/') + 13],
                                              file[file.rfind('/'):file.rfind('/') + 5]))
      
      noaas3.download_file("noaa-nexrad-level2", file,
                           outputDir + '/' + file[file.rfind('/') + 5:file.rfind('/') + 13] +
                           file[file.rfind('/'):file.rfind('/') + 5] + "/raw" + file[file.rfind('/'):])
   
   # Get NSE data for all new files
   if copyST or copyNRE: pullNSE(outputDir, filteredFiles, copyST, copyNRE)
   
   return 0

def getRadarListFromDomain(domains, radarFile, latName, lonName, radarCol):
   domains = [list(map(float, domain)) for domain in domains]
   
   for domain in domains:
      if not validDomain(domain):
         print("Domain {} not valid. Deleting from list".format(domain), flush = True)
         try:
            domains.remove(domain)
         except ValueError:
            pass
         else:
            break
   
   if len(domains) == 0:
      print("No valid domains. Exiting.", flush = True)
      exit()
   
   # Read in CSV and store as a dataframe
   try:
      rdf = pd.read_csv(radarFile, encoding = "ISO-8859-1")
   except OSError as err:
      print("Unable to read {}.".format(radarFile, err), flush = True)
      exit()
   
   if (latName != '') and (latName in rdf):
      lats = rdf[latName]
   elif "lat" in rdf:
      lats = rdf["lat"]
   elif "LAT" in rdf:
      lats = rdf["LAT"]
   elif "latitude" in rdf:
      lats = rdf["latitude"]
   elif "LATITUDE" in rdf:
      lats = rdf["LATITUDE"]
   elif "lats" in rdf:
      lats = rdf["lats"]
   elif "LATS" in rdf:
      lats = rdf["LATS"]
   elif "latitudes" in rdf:
      lats = rdf["latitudes"]
   elif "LATITUDES" in rdf:
      lats = rdf["LATITUDES"]
   else:
      print("Latitude column not found in {}.".format(radarFile), flush = True)
      exit()
   
   if (latName != '') and (latName in rdf):
      lons = rdf[latName]
   elif "lon" in rdf:
      lons = rdf["lon"]
   elif "LON" in rdf:
      lons = rdf["LON"]
   elif "longitude" in rdf:
      lons = rdf["longitude"]
   elif "LONGITUDE" in rdf:
      lons = rdf["LONGITUDE"]
   elif "lons" in rdf:
      lons = rdf["lons"]
   elif "LATS" in rdf:
      lons = rdf["LATS"]
   elif "longitudes" in rdf:
      lons = rdf["longitudes"]
   elif "LONGITUDES" in rdf:
      lons = rdf["LONGITUDES"]
   else:
      print("Longitude column not found in {}.".format(radarFile), flush = True)
      exit()
   
   if (radarCol != '') and (radarCol in rdf):
      rads = rdf[radarCol]
   elif "rda_id" in rdf:
      rads = rdf["rda_id"]
   elif "ICAO" in rdf:
      rads = rdf["ICAO"]
   else:
      print("Radar ID column not found in {}.".format(radarFile), flush = True)
      exit()
   
   # 0 = [upper lat], 1 =  [lower lat], 2 = [left lon], 3 = [right lon]
   print("Finding radars in domains:", flush = True)
   radars = [[rad for lat, lon, rad in zip(lats, lons, rads)
              if Point(lat, lon).within(polygon.Polygon([[domain[0], domain[2]],
                                                         [domain[0], domain[3]],
                                                         [domain[1], domain[3]],
                                                         [domain[1], domain[2]]]))]
              for domain in tqdm(domains, file = sys.__stdout__)]
      
   return radars

def validDomain(dom):   
   if (len(dom) == 4) and (dom[0] > dom[1]) and (dom[2] < dom[3]) and \
      (90 > dom[0] > -90) and (90 > dom[1] > -90) and \
      (180 > dom[2] > -180) and (180 > dom[3] > -180):
      return True
   else:
      return False

def validDate(date, dateFormat):
   try:
      time.strptime(date, dateFormat)
      return True
   except:
      return False
   
def validRadar(radar):
   if (len(radar) == 4) and ((radar[0] == 'K') or (radar[0] == 'P') or 
       (radar[0] == 'T')) and radar.isupper():
      return True
   else:
      return False

def useCSV(file, radarName, timeStampName, startTimeName, endTimeName, dateFormat, timeThreshold):         
   # Read in CSV and store as a dataframe
   try:
      df = pd.read_csv(file, encoding = "ISO-8859-1", low_memory = False)
   except OSError as err:
      print("Unable to read {}.".format(file, err), flush = True)
      exit()
   
   if not radarName in df or ((not timeStampName in df) and (not startTimeName in df or not endTimeName in df)) :
      print("\"{}\" or \"{}\" and \"{}\" or \"{}\" not valid column name in {}.".format(radarName, timeStampName, startTimeName, endTimeName, file), flush = True)
      exit()      
      
   if startTimeName in df and endTimeName in df:
      df = df.drop_duplicates([startTimeName, endTimeName, radarName])
   else:
      df = df.drop_duplicates([timeStampName, radarName])

   radars = [[str(radar)] if ' ' not in str(radar) else str(radar).split() for radar in df[radarName]]
   
   # Get window aroud each CSV entry and which radar the time is associated with
   if startTimeName in df and endTimeName in df:
      epochTime = np.array([{"start" : timegm(time.strptime(s, dateFormat)),
                             "end" : timegm(time.strptime(e, dateFormat)),
                             "radar" : rad} for s, e, radar in zip(df[startTimeName], df[endTimeName], radars)
                             for rad in radar if validRadar(rad) and validDate(s, dateFormat) and validDate(e, dateFormat)])  
   else:
      epochTime = np.array([{"start" : timegm(time.strptime(stamp, dateFormat)) - timeThreshold,
                             "end" : timegm(time.strptime(stamp, dateFormat)) + timeThreshold,
                             "radar" : rad} for stamp, radar in zip(df[timeStampName], radars)
                             for rad in radar if validRadar(rad) and validDate(stamp, dateFormat)])
   
   print(epochTime)
   return epochTime

def useRangeAndRadar(start, end, radars, dateFormat):
   # Go through radars and make sure that all formats are valid
   for radar in [item for sublist in radars for item in sublist]:
      if not validRadar(radar):
         if (len(radar) == 4) and ((radar[0] == 'k') or (radar[0] == 'p') or (radar[0] == 't')) \
            and radar.islower():
            radars = [[rad.replace(radar, radar.upper()) for rad in r] for r in radars]
            continue
         elif (len(radar) == 3) and radar.isupper():
            radars = [[rad.replace(radar, "K" + radar) for rad in r] for r in radars]
            continue          
         
         print("{} is not a valid radar. Deleting from list.".format(radar), flush = True)
         
         for rad in radars:
            try:
               rad.remove(radar)
            except ValueError:
               pass
            else:
               break
         
   if (len(radars) != len(start)) or (sum(map(len, start)) != sum(map(len, end))):
      print("There needs to be the same number of entries and/or start and end times. "
            "All radars may have been removed for a case if no valid radars were found.", flush = True)
      exit()
   
   # Get epoch time for each entry 
   epochTime = np.array([{"start" : timegm(time.strptime(s, dateFormat)),
                          "end" : timegm(time.strptime(e, dateFormat)), "radar" : rad}
                          for first, last, radar in zip(start, end, radars)
                          if ((len(radar) != 0) and (len(first) != 0))
                          for rad in radar for s, e in zip(first, last)
                          if validDate(s, dateFormat) and validDate(e, dateFormat)])
   return epochTime

def pullNSE(directory, files, copyST, copyNRE):
   
   dateformat  = re.compile("\d{4}(0[1-9]|1[0-2])(0[1-9]|1[0-9]|2[0-9]|3[0-1])")
   radarformat = re.compile("[K|P|T][A-Z]{3}")
   
   opaths = list(set(["{}/NSE/SoundingTable/{}/{}-{}*".format(re.search(dateformat, file).group(0),
                                                              re.search(radarformat, file).group(0),
                                                              re.search(dateformat, file).group(0),
                                                              re.search("_(.+?)\d{4}_", file).group(1))
                      for file in files]))
   
   opaths.sort()

   paths = [p for p in opaths if not glob("{}/{}".format(directory, p))]
   
   print("Downloading NSE files:", flush = True)
      
   for p in tqdm(paths, file = sys.__stdout__):
      indexs = False
      indexr = False
      
      try:
         yyyymmdd = re.search(dateformat, p).group(0)
         radar    = re.search(radarformat, p).group(0)
      except:
         print("\nFormat not as expected: {}".format(p), flush = True)
         continue
         
      if copyST:
         if not path.exists("{}/{}/NSE/SoundingTable/{}".format(directory, yyyymmdd, radar)):
            try:
               system("mkdir -p {}/{}/NSE/SoundingTable/{} > /dev/null 2>&1".format(directory, yyyymmdd, radar))
            except OSError as err:
               print("\nError making directory: {}/{}/NSE/SoundingTable/{}\n{}\n".format(directory, yyyymmdd, radar, err), flush = True)
               exit()
         
         try:
            print("\nrsync -auq wdssii@hwtarchive.hwt.nssl:/data6/NSE/{} "
                  "{}/{}/NSE/SoundingTable/{}/".format(p, directory, yyyymmdd, radar), flush = True)
            system("rsync -auq wdssii@hwtarchive.hwt.nssl:/data6/NSE/{} {}/{}/NSE/SoundingTable/{}/ "
                   "> /dev/null 2>&1".format(p, directory, yyyymmdd, radar))
            
            indexs = True
         except (KeyboardInterrupt):
            break
         except OSError as err:
            print(err, flush = True)
          
         if not glob("{}/{}".format(directory, p)):
            print("{}/{}".format(directory, p))
            try:
               print("\nrsync -auq wdssii@hwtarchive.hwt.nssl:/data6/NSE_Rebuild/NSE_organized/{} "
                     "{}/{}/NSE/SoundingTable/{}/".format(p, directory, yyyymmdd, radar), flush = True)
               system("rsync -auq wdssii@hwtarchive.hwt.nssl:/data6/NSE_Rebuild/NSE_organized/{} "
                      "{}/{}/NSE/SoundingTable/{}/ > /dev/null 2>&1".format(p, directory, yyyymmdd, radar))
               
               indexs = True
            except (KeyboardInterrupt):
               break
            except OSError as err:
               print(err, flush = True)
            
            if not glob("{}/{}".format(directory, p)):
               try:
                  p.replace("/NSE/SoundingTable", "/data6/NSE_Rebuild/NSE/SoundingTable")
                  
                  print("\nrsync -auq wdssii@hwtarchive.hwt.nssl:/data6/NSE_Rebuild/NSE_organized/{} "
                        "{}/{}/NSE/SoundingTable/{}/".format(p, directory, yyyymmdd, radar), flush = True)
                  system("rsync -auq wdssii@hwtarchive.hwt.nssl:/data6/NSE_Rebuild/NSE_organized/{} "
                         "{}/{}/NSE/SoundingTable/{}/ > /dev/null 2>&1".format(p, directory, yyyymmdd, radar))
                  
                  indexs = True
               except (KeyboardInterrupt):
                  break
               except OSError as err:
                  print(err, flush = True)
                  print("\nCould not find any SoundingTable for: \n{}\n".format(p))
               
               if not glob("{}/{}".format(directory, p)):
                  print("Could not find {} at hwtarchive.".format(p))
                  indexs = False
                        
      # Near radar environment table
      if copyNRE:
         p.replace("/NSE/SoundingTable", "/NSE/NearRadarEnvironmentTable")
         
         if not path.exists("{}/{}/NSE/NearRadarEnvironmentTable/{}".format(directory, yyyymmdd, radar)):
            try:
               system("mkdir -p {}/{}/NSE/NearRadarEnvironmentTable/{} > /dev/null 2>&1".format(directory, yyyymmdd, radar))
            except OSError as err:
               print("\nError making directory: {}/{}/NSE/NearRadarEnvironmentTable/{}\n{}\n".format(directory, yyyymmdd, radar, err), flush = True)
               exit()
         
         try:
            print("\nrsync -auq wdssii@hwtarchive.hwt.nssl:/data6/NSE/{} "
                  "{}/{}/NSE/NearRadarEnvironmentTable/{}/".format(p, directory, yyyymmdd, radar), flush = True)
            system("rsync -auq wdssii@hwtarchive.hwt.nssl:/data6/NSE/{} {}/{}/NSE/NearRadarEnvironmentTable/{}/ "
                   "> /dev/null 2>&1".format(p, directory, yyyymmdd, radar))
            
            indexr = True
         except (KeyboardInterrupt):
            break
         except OSError as err:
            print(err, flush = True)
          
         if not glob("{}/{}".format(directory, p)):
            print("{}/{}".format(directory, p))
            try:
               print("\nrsync -auq wdssii@hwtarchive.hwt.nssl:/data6/NSE_Rebuild/NSE_organized/{} "
                     "{}/{}/NSE/NearRadarEnvironmentTable/{}/".format(p, directory, yyyymmdd, radar), flush = True)
               system("rsync -auq wdssii@hwtarchive.hwt.nssl:/data6/NSE_Rebuild/NSE_organized/{} "
                      "{}/{}/NSE/NearRadarEnvironmentTable/{}/ > /dev/null 2>&1".format(p, directory, yyyymmdd, radar))
               
               indexr = True
            except (KeyboardInterrupt):
               break
            except OSError as err:
               print(err, flush = True)
            
            if not glob("{}/{}".format(directory, p)):
               try:
                  p.replace("/NSE/NearRadarEnvironmentTable", "/data6/NSE_Rebuild/NSE/NearRadarEnvironmentTable")
                  
                  print("\nrsync -auq wdssii@hwtarchive.hwt.nssl:/data6/NSE_Rebuild/NSE_organized/{} "
                        "{}/{}/NSE/NearRadarEnvironmentTable/{}/".format(p, directory, yyyymmdd, radar), flush = True)
                  system("rsync -auq wdssii@hwtarchive.hwt.nssl:/data6/NSE_Rebuild/NSE_organized/{} "
                         "{}/{}/NSE/NearRadarEnvironmentTable/{}/ > /dev/null 2>&1".format(p, directory, yyyymmdd, radar))

                  
                  indexr = True
               except (KeyboardInterrupt):
                  break
               except OSError as err:
                  print(err, flush = True)
                  print("\nCould not find any NearRadarEnvironmentTable for: \n{}\n".format(p))
               
               if not glob("{}/{}".format(directory, p)):
                  print("Could not find {} at hwtarchive.".format(p))
                  indexr = False
   
      if indexs or indexr: system("makeIndex.pl {}/{}/NSE code_index.xml > /dev/null 2>&1".format(directory, yyyymmdd))
 
   return 0

if __name__ == "__main__":
   parser = argparse.ArgumentParser(description = "Downloads NEXRAD level II radar data "
            "from Amazon within time threshold of csv entry or within date/radar lists, "
            "and gets NSE data from hwtarchive if available. To specify more than one "
            "case/radar by date/radar, add another optional argument to the command. "
            "Example: python downloadLevel2RadarData.py --ds 20130520-2012 20130520-2345 "
            "--ds 20130521-0530 --de 20130520-2030 20130520-2350 --de 20130521-0600 --rad KFDR "
            "--rad KTLX KVNX . Come ask me (Thea) if you're confused!")
   parser.add_argument("-d", metavar = "dateFormat", type = str, nargs = '?',
                       default = "%Y%m%d-%H%M", help = "String to describe date "
                       "format for the timestamp in the CSV. Default = %(default)s")
   parser.add_argument("--ds", metavar = "startDates", type = str, nargs = '*',
                       default = [], action = 'append', help = "List of start dates")
   parser.add_argument("--de", metavar = "endDates", type = str,  nargs = '*',
                       default = [], action = 'append', help = "List of end dates.")
   parser.add_argument("--dom", metavar = "domains", type = str,  nargs = '*',
                       default = [], action = 'append', help = "List of domains "
                       "defined like [upper lat] [lower lat] [left lon] [right lon]. "
                       "Example: --dom 40.5 39.8 -100 -98.4")
   parser.add_argument("-i", metavar = "inputFile", type = str, nargs = '?', default = '',
                       help = "Path to csv with reports. This needs to have a column named "
                       "\"timestamp\" with format: \"YYYYmmdd-HHMM\" and one named \"radar\" "
                       "if you use the default settings.")
   parser.add_argument("--ist", metavar = "startTimeName", type = str, nargs = '?',
                       default = "startDate", help = "Column name for the radar from "
                       "the CSV. Default = %(default)s.")
   parser.add_argument("--iet", metavar = "endTimeName", type = str,  nargs = '?',
                       default = "endDate", help = "Column name for the time stamp "
                       "from the CSV. Default = %(default)s.")
   parser.add_argument("--ir", metavar = "radarName", type = str, nargs = '?',
                       default = "radar", help = "Column name for the radar from "
                       "the CSV. Default = %(default)s.")
   parser.add_argument("--it", metavar = "timeStampName", type = str,  nargs = '?',
                       default = "timestamp", help = "Column name for the time stamp "
                       "from the CSV. Default = %(default)s.")
   parser.add_argument("--nst", metavar = "copyST", type = bool, nargs = '?', default = False,
                       help = "If true, get NSE SoundingTable data from hwtarchive. "
                       "Default = %(default)s")
   parser.add_argument("--nre", metavar = "copyNRE", type = bool, nargs = '?', 
                       default = False, help = "If true, get NSE near radar environment "
                       "data from hwtarchive. Only works if -n is True. Default = %(default)s")
   parser.add_argument("-o", metavar = "outputDir", type = str,  nargs = '?', 
                       default = "temp", help = "Path to output directory. Default = %(default)s")
   parser.add_argument("-p", metavar = "printFileList", type = bool, nargs = '?', default = False,
                       help = "Print list of files that will be downloaded. Default = %(default)s.")
   parser.add_argument("--rad", metavar = "radars", type = str, nargs = '*', default = [],
                       action = 'append', help = "Radar names for specifying cases. "
                       "Can be entered as list of lists if multiple radars per "
                       "start/end date pairs are desired.")
   parser.add_argument("--rf", metavar = "radarFile", type = str, nargs = '?', default = '',
                       help = "Path to CSV with radar information.")
   parser.add_argument("--rt", metavar = "latName", type = str, nargs = '?', default = '',
                       help = "Specify latitude column name for list of radars if "
                       "the column name is not one of the following (all upper or lower case): "
                       "lat, lats, latitude, latitudes.")
   parser.add_argument("--rn", metavar = "lonName", type = str, nargs = '?', default = '',
                       help = "Specify longitude column name for list of radars "
                       "if the column name is not one of the following (all upper or lower case): "
                       "lon, lons, longitude, longitudes.")
   parser.add_argument("--rr", metavar = "radarCol", type = str, nargs = '?', default = '',
                       help = "Specify radar column name for list of radars if "
                       "the column name is not one of the following: rda_id, ICAO.")
   defaultValue = 300
   msg = u"Time window around entry time to be included in download. "
   "The script will download all files +- %(default)d s ({} min) by default".format(defaultValue/60)
   parser.add_argument("-t", metavar = "timeThreshold", type = int, nargs = '?', 
                       default = defaultValue, help = msg)
   args = parser.parse_args(sys.argv[1:])
       
   main(outputDir = args.o, dateFormat = args.d, startDates = args.ds, endDates = args.de,
        inputFile = args.i, copyST = args.nst, copyNRE = args.nre, printFileList = args.p,
        radars = args.rad, startTimeName = args.ist, endTimeName = args.iet, radarName = args.ir, timeStampName = args.it, timeThreshold = args.t,
        domains = args.dom, radarFile = args.rf, latName = args.rt, lonName = args.rn, radarCol = args.rr)
   