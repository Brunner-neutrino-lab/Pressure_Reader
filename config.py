#This file contains the global variables to be used in the bvl-pyomngodb module
import os

#Global variable definitions
MONGO_HOST = ('132.206.126.208',1337)   #IP and Port of the MongoDB Instance PC
MONGO_USER = 'bvl-db'                   #Name of user to SSH as
MAIN_DIR = os.getcwd() # need absolute paths for LabVIEW
DATA_FOLDER = MAIN_DIR+ '/data/'                   #Name of the folder where the data files to be uploaded are written
DOWNLOAD_FOLDER = MAIN_DIR + '/data/download/'      #Name of the folder where the download files will be saved 
CONFLICT_FOLDER = MAIN_DIR + '/data/db-conflict/'   #Name of the folder to store conflict files
CSV_FILE_NAME = 'bvl_data_upload.csv'   #Name of the csv file in which data is to be stored
UPLOAD_LOG_FILE = 'data_upload_log.txt' #Name of the txt file where the latest successful upload date is stored
SAVE_FILE_NAME = 'bvl_data_download.csv'#Name of the csv file where the downloaded data is to be stored
PKEY_FILE_PATH = os.path.expanduser('~/.ssh/id_rsa') #Full path to the private RSA key
STD_CUT_PATH = MAIN_DIR+'/std_cuts.txt' #Path to the std_cuts file

# sensor toggling:
# 1 = on, 0 = off
ExhaustStatus = 1
GasStatus = 1
leds = 1 
email = 1         

Interval_Between_Scans = 0.5 # in minutes
Upload_Interval_Exhaust = 1 # in minutes 
Upload_Interval_Gas = 480 # in minutes
csv_path = 'data/bvl_data_upload.csv'  #Path to csv file
dbname = 'Bvl_Pressure_Sensors'#how the data will show up on the bvl database
# need to change collection 
collection_exhaust ='Exhaust_Pressures' #subcategory of dbname
collection_gas ='Compressed_Gas_Pressures' #subcategory of dbname

# for sparsifictaion
std_dict = {
    'exhaust_psig': 0.1,   # Standard deviation threshold for exhaust
    'compressed_gas_psig': 0.1,   # Standard deviation threshold for gas
}