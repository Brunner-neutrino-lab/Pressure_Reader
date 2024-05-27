import numpy
import pandas
import sys
import os
import datetime
import bvl_pymongodb
import random
import time
import config as c
import sensors as s

#Path to csv file
csv_path=c.DATA_FOLDER+c.CSV_FILE_NAME

#Size of buffer to store data for terminal upload (can hold 1hr worth of data)
TerminalBufferSamples=int(60*60/c.Interval_Between_Scans)

#Creating our list of headers
#We will upload the timestamp which corresponds to the time of the datapoint, 
dfheader=['timestamp','exhaust_psig','compressed_gas_psig']

#Creating the dataframe used for uploads as well as the bufferDataFrame
df=pandas.DataFrame(columns=dfheader)
bufferDataFrame=df.copy()

print("Starting DAQ loop")
while True:

    try:
        df = pandas.DataFrame(columns=dfheader) #Clean data frame with only the header
        
        maxSamples_exhaust = int(c.Upload_Interval_Exhaust*60/c.Interval_Between_Scans) #upload to DB every `uploadInterval` mins
        maxSamples_gas = int(c.Upload_Interval_Gas*60/c.Interval_Between_Scans)
        for i in range(maxSamples_exhaust): 

            timenow = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] #get current time and add to list
            
            data = [timenow] # Start a clean list with first entry as timestamp
            
            #Appending the scalar+noise component
            data.append(s.exhaustpressure())
            
            df.loc[i,:] = data # Append the data list to the end of the data frame as a row 
            
            #Append the last entry to the buffer (for live monitoring over longer than 5 mins timescales... 
            bufferDataFrame = bufferDataFrame.append(df.tail(1), ignore_index=True).tail(TerminalBufferSamples)        
            
            if(i==0 or (i % int(maxSamples_exhaust/c.Upload_Interval_Exhaust) == 0)): #output stuff on screen every ~1 min
                print(bufferDataFrame.tail(5).to_markdown()) #Print last five items in data frame
                t = maxSamples_exhaust - i
                print("\n  %d of %d samples taken. %d more before upload to database. \n\n"%(i, maxSamples, t)) 
            elif(i%10==0):
                t = maxSamples_exhaust - i
                print("\n  %d of %d samples taken. %d more before upload to database. \n\n"%(i, maxSamples, t)) 
            
            time.sleep(c.Interval_Between_Scans) 
        for i in range(maxSamples_gas): 

            timenow = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3] #get current time and add to list
            
            data = [timenow] # Start a clean list with first entry as timestamp
            
            #Appending the scalar+noise component
            data.append(s.gaspressure())
            
            df.loc[i,:] = data # Append the data list to the end of the data frame as a row 
            
            #Append the last entry to the buffer (for live monitoring over longer than 5 mins timescales... 
            bufferDataFrame = bufferDataFrame.append(df.tail(1), ignore_index=True).tail(TerminalBufferSamples)        
            
            if(i==0 or (i % int(maxSamples_gas/c.Upload_Interval_Gas) == 0)): #output stuff on screen every ~1 min
                print(bufferDataFrame.tail(5).to_markdown()) #Print last five items in data frame
                t = maxSamples_gas - i
                print("\n  %d of %d samples taken. %d more before upload to database. \n\n"%(i, maxSamples, t)) 
            elif(i%10==0):
                t = maxSamples_gas - i
                print("\n  %d of %d samples taken. %d more before upload to database. \n\n"%(i, maxSamples, t)) 
            
            time.sleep(c.Interval_Between_Scans) 

        # After maxSamples, i.e. `uploadInterval` mins has passed, save this dataframe to a csv for upload to DB.
        df.to_csv(csv_path, index=False)

        #Upload the file to the database (it will be wiped)
        err_dict = bvl_pymongodb.upload_from_csv(c.db_name, c.collection)
        print("\n\n Uploaded to BvL DB, err_dict:", err_dict)


    except KeyboardInterrupt:
        # Need to upload any remaining data to db here, and check for nothing uploaded since last upload using db_upload_data_log.txt
        
        print(bufferDataFrame.tail(10).to_markdown()) #Print last ten items in data frame
        print("\n\n") #Print last five items in data frame
        print("Excuse me? Why'd you keyboard interrupt me??") #Print last five items in data frame
        break