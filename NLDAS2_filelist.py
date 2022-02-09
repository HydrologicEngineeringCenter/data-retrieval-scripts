import re
import requests

###  The following steps only need to be done once
#  First, create a NASA EarthData account
#  Next, link GES DISC with your EarthData account: https://disc.gsfc.nasa.gov/earthdata-login
#  Then on your local machine, create a .netrc file placed at C:\Users\<username>\.netrc
#   .netrc is a text file with one line using your EarthData login: machine urs.earthdata.nasa.gov login <uid> password <password>

###  The following script works for links lists provided by EarthData GES DISC
#  Make sure to update the URLList string to the downloaded links list file
#  Also make sure to update the string in FILENAME on line 21 to the folder path for the download

URLList = "C:/Users/HEC/Downloads/subset_NLDAS_FORA0125_H_2.0_20220208_144246.txt"

with open(URLList) as file:
    lines = file.readlines()

for URL in lines:
    label = re.search('&LABEL=(.*)&SHORTNAME=', URL)
    FILENAME = 'C:/Users/HEC/Downloads/NLDAS2/' + label.group(1)
    result = requests.get(URL)

    try:
        result.raise_for_status()
        f = open(FILENAME, 'wb')
        f.write(result.content)
        f.close()
        print('contents of URL written to ' + FILENAME)
    except:
        print('requests.get() returned an error code ' + str(result.status_code))