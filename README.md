# downloadLevel2RadarData

Downloads NEXRAD level II radar data from AWS within domain/radar time range/threshold of csv entry or command options, and gets NSE data from hwtarchive if available (with progress bars!). To specify more than one case/radar by date/radar, add another optional argument to the command. This script will also check specified output folder whether the files already exist, saving time by avoiding re-downloading existing data. Type "python downloadLevel2RadarData.py -h" to see options.

If using csv, you can specify radars (space-separated for more than one radar for one case) or a domain (supply nexrad-info.csv file), as well as a specific time (where you set the time threshold yourself/default is 5 min) or a time range.

Example: python downloadLevel2RadarData.py --ds 20130520-2012 20130520-2345 --ds 20130521-0530 --de 20130520-2030 20130520-2350 --de 20130521-0600 --rad KFDR --rad KTLX KVNX 

Ask me (Thea) if you're confused!
