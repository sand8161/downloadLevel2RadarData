import argparse, boto3, time
import numpy as np
import pandas as pd
from botocore import UNSIGNED
from botocore.client import Config
from calendar import timegm
from datetime import date, timedelta
from glob import glob
from os import path, system
from sys import argv
from tqdm import tqdm

def main(outputDir = '/localdata/temp', dateFormat = "%Y%m%d-%H%M", startDates = [], endDates = [], inputFile = '', copyNSE = True, copyNRE = True, printFileList = False, radars = [], radarName = "radar", timeStampName = "timestamp", timeThreshold = 300):   
   if not path.exists(outputDir):
      print("Output directory {} does not exist. Creating directory.".format(outputDir))
      system("mkdir -p {}".format(outputDir))      
   
   if not "%Y" or not "%m" or not "%d" in dateFormat:
      print("{} is not a valid date format".format(dateFormat))
      return 0

   if inputFile == '':
      if len(startDates) == 0 or len(endDates) == 0 or len(radars) == 0:
         print("One of the range/radar variables is empty and no CSV is specified.")
         return 0
      epochTime = useRangeAndRadar(startDates, endDates, radars, dateFormat)
   else:
      if not path.exists(inputFile):
         print("Input file {} does not exist.".format(inputFile))
         return 0
   
      if timeThreshold < 0:
         print("Time threshold {} cannot be negative.".format(timeThreshold))
         return 0
      epochTime = useCSV(inputFile, radarName, timeStampName, dateFormat, timeThreshold)
   
   # Make sure there is input data
   if len(epochTime) == 0:
      print("No valid data was found. Check that your date/radar format is valid.")
      return 0
   
   # Split into more entries if start and end times are more than 1 day apart
   # Make list of prefixes to find the relevant objects from Amazon bucket 
   prefixList = np.array([[(date.fromtimestamp(item["start"]) + timedelta(days = numDays)).strftime("%Y/%m/%d/") + item["radar"] + '/'] for item in epochTime for numDays in range((date.fromtimestamp(item["end"]) - date.fromtimestamp(item["start"])).days + 1)])
   # Flatten and find unique entries in list
   prefixList = np.array(list(set([item for sublist in prefixList for item in sublist])))
   
   # Setup Anoymous Login for S3 with Amazon 
   noaas3 = boto3.client("s3",region_name="us-east-1",config=Config(signature_version=UNSIGNED))
   
   # Make list of files in the bucket for the dates specified
   print("Finding files to download:")
   objectsInBucket = np.array([noaas3.list_objects_v2(Bucket='noaa-nexrad-level2', Delimiter='/', Prefix=prefix) for prefix in tqdm(prefixList)])
   objectList = np.array([item["Contents"] for item in objectsInBucket if "Contents" in item])
   # Make sure files are of the right format
   fileNames = np.array([file["Key"] for object in objectList for file in object if "Key" in file and (file["Key"][file["Key"].rfind('/') + 1:file["Key"].rfind('/') + 13] == file["Key"][file["Key"].rfind('/') - 4:file["Key"].rfind('/')] + file["Key"][0:4] + file["Key"][5:7] + file["Key"][8:10]) and (file["Key"][-3:] != "MDM") and (file["Key"][-4:] != ".001")])
   
   # Make dictionary with the file name, radar, and UNIX time for each file
   fileInfo = np.array([{"name" : name, "radar" : name[name.rfind('/') + 1:name.rfind('/') + 5], "time" : timegm(time.strptime(name[name.rfind('/') + 5:name.rfind('/') + 20], "%Y%m%d_%H%M%S"))} for name in fileNames])
     
   radars = np.array([entry["radar"] for entry in fileInfo])
   
   # Filtering out files outside of the time window
   print("Filtering files to download:")
   filteredFiles = [info["name"] for epoch in tqdm(epochTime) for info in fileInfo[np.where(radars == epoch["radar"])[0]] if (epoch["start"] <= info["time"] <= epoch["end"])] 
      
   # Make set of existing radar files in specified output directory for skipping downloading existing files
   print("Finding exisiting files (this might take a little while [without a progress bar] if you have many files in your output directory):")
   existingFiles = set([item[item.rfind('/') + 5:item.rfind('/') + 9] + '/' + item[item.rfind('/') + 9:item.rfind('/') + 11] + '/' + item[item.rfind('/') + 11:item.rfind('/') + 13] + '/' + item[item.rfind('/') + 1:item.rfind('/') + 5] + '/' + item[item.rfind('/') + 1:] for item in tqdm(glob(outputDir + "/*/*/raw/*"))])
   
   # Filtering out files that are already in output directory
   filesToDownload = np.array(list(set(filteredFiles) - existingFiles))
      
   # Return if no files need to be downloaded
   if len(filesToDownload) == 0:
      print("No files to download for {}".format(inputFile))
      return 0
   
   # Print list of files to download if this setting is set
   if printFileList:
      print("Files to download:")
      for file in filesToDownload: print(file)
      
   # Download files and make directories if they don't already exist
   print("Downloading files:")
   for file in tqdm(filesToDownload):
      if not path.exists(outputDir + '/' + file[file.rfind('/') + 5:file.rfind('/') + 13] + file[file.rfind('/'):file.rfind('/') + 5] + "/raw"):
         system("mkdir -p {}/{}{}/raw".format(outputDir,file[file.rfind('/') + 5:file.rfind('/') + 13],file[file.rfind('/'):file.rfind('/') + 5]))
      noaas3.download_file("noaa-nexrad-level2", file, outputDir + '/' + file[file.rfind('/') + 5:file.rfind('/') + 13] + file[file.rfind('/'):file.rfind('/') + 5] + "/raw" + file[file.rfind('/'):])
   
   # Get NSE data for all new files
   if copyNSE: pullNSE(outputDir, filesToDownload, copyNRE)

   return 0

def validDate(date, dateFormat):
   try:
      time.strptime(date, dateFormat)
      return True
   except:
      return False
   
def validRadar(radar):
   if (len(radar) == 4) and (radar[0] == 'K') and radar.isupper():
      return True
   else:
      return False

def useCSV(file, radarName, timeStampName, dateFormat, timeThreshold):         
   # Read in CSV and store as a dataframe
   try:
      df = pd.read_csv(file, encoding = "ISO-8859-1", low_memory = False)
   except OSError as err:
      print("Unable to read {}.".format(file,err))
      exit()
   
   if not radarName in df or not timeStampName in df:
      print("{} or {} not valid column name in {}.".format(radarName, timeStampName, file))
      exit()      
   
   df = df.drop_duplicates([timeStampName])
   
   radars = [[radar] if ' ' not in radar else radar.split() for radar in df[radarName]]
   
   # Get window aroud each CSV entry and which radar the time is associated with
   epochTime = np.array([{"start" : timegm(time.strptime(stamp, dateFormat)) - timeThreshold, "end" : timegm(time.strptime(stamp, dateFormat)) + timeThreshold, "radar" : rad} for stamp, radar in zip(df[timeStampName], radars) for rad in radar if validRadar(rad) and validDate(stamp, dateFormat)])

   return epochTime

def useRangeAndRadar(start, end, radars, dateFormat):
   # Go through radars and make sure that all formats are valid
   for radar in [item for sublist in radars for item in sublist]:
      if not validRadar(radar):
         if (len(radar) == 4) and (radar[0] == 'k') and radar.islower():
            radars = [[rad.replace(radar,radar.upper()) for rad in r] for r in radars]
            continue
         elif (len(radar) == 3) and radar.isupper():
            radars = [[rad.replace(radar,"K" + radar) for rad in r] for r in radars]
            continue          
         
         print("{} is not a valid radar. Deleting from list.".format(radar))
         
         for rad in radars:
            try:
               rad.remove(radar)
            except ValueError:
               pass
            else:
               break
         
   if (len(radars) != len(start)) or (sum(map(len, start)) != sum(map(len, end))):
      print("There needs to be the same number of entries and/or start and end times. All radars may have been removed for a case if no valid radars were found.")
      exit()
   
   # Get epoch time for each entry 
   epochTime = np.array([{"start" : timegm(time.strptime(s, dateFormat)), "end" : timegm(time.strptime(e, dateFormat)), "radar" : rad} for first, last, radar in zip(start, end, radars) if ((len(radar) != 0) and (len(first) != 0)) for rad in radar for s, e in zip(first, last) if validDate(s, dateFormat) and validDate(e, dateFormat)])
         
   return epochTime

def pullNSE(directory, files, copyNRE):
   print("Downloading NSE files:")
   for file in tqdm(files):
      # Does the index need to be made?
      index = False
      
      # Find relevant info for the current file
      radar = file[file.rfind('/') + 1:file.rfind('/') + 5]
      date  = file[file.rfind('/') + 5:file.rfind('/') + 13]
      hour  = file[file.rfind('/') + 14:file.rfind('/') + 16]
      
      # Set paths to sounding tables and near radar environment on hwtarchive
      hwtSTPath  = "/data6/NSE/" + date + "/NSE/SoundingTable/" + radar + '/' + date + '-' + hour + '*'
      hwtNREPath = "/data6/NSE/" + date + "/NSE/NearRadarEnvironmentTable/" + date + '-' + hour + '*'

      # Get NSE data if it is not already in the specified directory:
      
      # Sounding table
      if not path.exists(directory + '/' + date + "/NSE/SoundingTable/" + radar):
         try:
            system("mkdir -p {}/{}/NSE/SoundingTable/{}".format(directory,date,radar))
         except OSError as err:
            print("***ERROR MAKING DIRECTORY***: {}/{}/NSE/SoundingTable/{}\n{}\n".format(directory,date,radar,err))
         try:
            system("rsync -auq wdssii@hwtarchive:{} {}/{}/NSE/SoundingTable/{}".format(hwtSTPath,directory,date,radar))
         except (KeyboardInterrupt):
            break
         except OSError as err:
            print(err)
            print("SoundingTable for {} @ {} UTC is not available!".format(date,hour))
         index = True
      else:
         if not glob("{}/{}/NSE/SoundingTable/{}/{}-{}*".format(directory,date,radar,date,hour)):
            try:
               system("rsync -auq wdssii@hwtarchive:{} {}/{}/NSE/SoundingTable/{}".format(hwtSTPath,directory,date,radar))
            except (KeyboardInterrupt):
               break
            except OSError as err:
               print(err)
               print("SoundingTable for {} @ {} UTC is not available!".format(date,hour))
            index = True
      
      # Near radar environment table
      if copyNRE:
         if not path.exists(directory  + "/" + date + "/NSE/NearRadarEnvironmentTable"):
            try:
               system("mkdir -p {}/{}/NSE/NearRadarEnvironmentTable".format(directory, date))
            except OSError as err:
               print("***ERROR MAKING DIRECTORY***: {}/{}/NSE/NearRadarEnvironmentTable\n{}\n".format(directory,date,err))
            try:
               syscmd = "rsync -auq wdssii@hwtarchive:{} {}/{}/NSE/NearRadarEnvironmentTable".format(hwtNREPath,directory,date)
               system(syscmd)
            except (KeyboardInterrupt):
               break
            except OSError as err:
               print(err)
               print("NearRadarEnvironmentTable for {} @ {} UTC is not available!".format(date,hour))
            index = True
         else:
            if not glob("{}/{}/NSE/NearRadarEnvironmentTable/{}-{}*".format(directory,date,date,hour)):
               try:
                  syscmd = "rsync -auq wdssii@hwtarchive:{} {}/{}/NSE/NearRadarEnvironmentTable".format(hwtNREPath,directory,date)
                  system(syscmd)
               except (KeyboardInterrupt):
                  break
               except OSError as err:
                  print(err)
                  print("NearRadarEnvironmentTable for {} @ {} UTC is not available!".format(date,hour))
               index = True
   
      if index: system("w2makeindex.py {}/{}/NSE -xml".format(directory,date))
 
   return 0

if __name__ == "__main__":
   parser = argparse.ArgumentParser(description = "Downloads NEXRAD level II radar data from Amazon within time threshold of csv entry or within date/radar lists, and gets NSE data from hwtarchive if available. To specify more than one case/radar by date/radar, add another optional argument to the command. Example: python downloadRadarDataFromCSV.py --ds 20130520-2012 20130520-2345 --ds 20130521-0530 --de 20130520-2030 20130520-2350 --de 20130521-0600 --rad KFDR --rad KTLX KVNX . Come ask me (Thea) if you're confused!")
   parser.add_argument("-d",    metavar = "dateFormat",    type = str,  nargs = '?', default = "%Y%m%d-%H%M",         help = "String to describe date format for the timestamp in the CSV.")
   parser.add_argument("--ds",  metavar = "startDates",    type = str,  nargs = '*', default = [], action = 'append', help = "List of start dates")
   parser.add_argument("--de",  metavar = "endDates",      type = str,  nargs = '*', default = [], action = 'append', help = "List of end dates.")
   parser.add_argument("-i",    metavar = "inputFile",     type = str,  nargs = '?', default = '',                    help = "Path to csv with reports. This needs to have a column named \"timestamp\" with format: \"YYYYmmdd-HHMM\" and one named \"radar\" if you use the default settings.")
   parser.add_argument("-n",    metavar = "copyNSE",       type = bool, nargs = '?', default = True,                  help = 'If true, get NSE data from hwtarchive.')
   parser.add_argument("--nre", metavar = "copyNRE",       type = bool, nargs = '?', default = False,                 help = 'If true, get NSE near radar environment data from hwtarchive. Only works if -n is True.')
   parser.add_argument("-o",    metavar = "outputDir",     type = str,  nargs = '?', default = '/localdata/temp',     help = 'Path to output directory.')
   parser.add_argument("-p",    metavar = "printFileList", type = bool, nargs = '?', default = False,                 help = "Print list of files that will be downloaded. Default = False.")
   parser.add_argument("--rad", metavar = "radars",        type = str,  nargs = '*', default = [], action = 'append', help = "Radar names for specifying cases. Can be entered as list of lists if multiple radars per start/end date pairs are desired.")
   parser.add_argument("--rn",  metavar = "radarName",     type = str,  nargs = '?', default = "radar",               help = "Column name for the radar from the CSV. Default = \"radar\".")
   parser.add_argument("-s",    metavar = "timeStampName", type = str,  nargs = '?', default = "timestamp",           help = "Column name for the time stamp from the CSV. Default = \"timestamp\".")
   parser.add_argument("-t",    metavar = "timeThreshold", type = int,  nargs = '?', default = 300,                   help = u"Time window around entry time to be included in download. The script will download all files \u00B1 300 s (5 min) by default.")    
   args = parser.parse_args(argv[1:])
       
   main(outputDir = args.o, dateFormat = args.d, startDates = args.ds, endDates = args.de, inputFile = args.i, copyNSE = args.n, copyNRE = args.nre, printFileList = args.p, radars = args.rad, radarName = args.rn, timeStampName = args.s, timeThreshold = args.t)
   