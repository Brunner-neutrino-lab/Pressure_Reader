import time # for time intervals
import config # the config file
import functions # fucntions for lights and database
import sensors as s # functions for sensor data
import bvl_pymongodb # stuff to upload to database
import threading # for the tkinter loop
import tkinter as tk # for gui
import pandas as pd # for datafram (good for handling the data)
import datetime # for the time now


# Create the main window for displaying values
m = tk.Tk() # creates window (m)
m.title('Pressure Values')

# Create labels to display titles above exhaust and compressed gas values
exhaust_title_label = tk.Label(m, text="Exhaust Pressure (psi)", font=('Arial', 25))
exhaust_title_label.grid(row=0, column=0, padx=10, pady=(10,0))

compressed_gas_title_label = tk.Label(m, text="Compressed Gas Pressure (psi)", font=('Arial', 25))
compressed_gas_title_label.grid(row=0, column=1, padx=10, pady=(10,0))

# Create labels to display exhaust and compressed gas values with larger font size
font_size = 60
exhaust_label = tk.Label(m, text="0.00", font=('Arial', font_size))
exhaust_label.grid(row=1, column=0, padx=10, pady=10)

compressed_gas_label = tk.Label(m, text="0.00", font=('Arial', font_size))
compressed_gas_label.grid(row=1, column=1, padx=10, pady=10)

# Create labels for happy/sad faces (see functions.py)
#psig_face_label = tk.Label(m, text="😊", font=('Arial', 60))
#psig_face_label.grid(row=2, column=0)

#compressed_gas_face_label = tk.Label(m, text="😊", font=('Arial', 60))
#compressed_gas_face_label.grid(row=2, column=1)

# Initialize the variable to control the update loop
stopped = False

# Bind the closing event to the on_closing function
m.protocol("WM_DELETE_WINDOW", functions.on_closing)

# Start updating the values
functions.update_values()

# there are also headers as functions (optional to use instead)
dfheader_exhaust=['timestamp','exhaust_psig']
dfheader_gas=['timestamp','compressed_gas_psig']
df_exhaust=pd.DataFrame(columns=dfheader_exhaust)
df_gas=pd.DataFrame(columns=dfheader_gas)
# Merging only the timestamp column from df_exhaust and the second column from df_gas
bufferDataFrame = pd.DataFrame({'timestamp': df_exhaust['timestamp'], 
                                'exhaust_psig': df_exhaust.iloc[:, 1], 
                                'compressed_gas_psig': df_gas.iloc[:, 1]})

TerminalBufferSamples=int(60*60) # one hour

def main_loop():
    StartTimeExhaust=time.time() #Start and End time will be used to calculate the time ellapsed since the program started
    StartTimeGas=time.time()
    print('Starting!')
    while True: # the while true loop will continue every scan interval (found in config)
        try:
            EndTime=time.time() # use endtime to get time elasped
            
            if (EndTime-StartTimeExhaust)<60*config.Upload_Interval_Exhaust:
                timenow = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") # timestamp
                data = [timenow] # sets the first input to be the timestamp
                data.append(s.exhaustpressure()) # appends the pressure in the next column
                
                df_exhaust.loc[len(df_exhaust)] = data  # Append the data list to the end of the data frame as a row

                # appends to buffer
                bufferDataFrame = bufferDataFrame.append(df_exhaust.tail(1), ignore_index=True).tail(TerminalBufferSamples)

                # when time exceeds upload interval, it will upload to database
                if (EndTime-StartTimeExhaust)>=60*config.Upload_Interval_Exhaust:
                    # sparsify data
                    filtereddf_exhaust = bvl_pymongodb.sparsify_data(df_exhaust, config.std_dict)
                    filtereddf_exhaust.to_csv(config.csv_path, index=False)
                    bvl_pymongodb.upload_from_csv(config.dbname,config.collection_exhaust)#this part uploads it, but then deleted everything in the file
                    StartTimeExhaust = time.time() # reset timer
                    print('Exhaust Uploaded!')
                else:
                    continue
            # this is for compressed gas
            if (EndTime-StartTimeGas)<60*config.Upload_Interval_Gas:#The time in minutes between uploads.
                timenow = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S") # timestamp
                data = [timenow] # timestamp in first column
                data.append(s.gaspressure()) # appends to the next column
                
                df_gas.loc[len(df_gas)] = data  # Append the data list to the end of the data frame as a row

                # appends to buffer
                bufferDataFrame = bufferDataFrame.append(df_gas.tail(1), ignore_index=True).tail(TerminalBufferSamples)

                # when time exceeds upload interval, uploads to database
                if (EndTime-StartTimeGas)>=60*config.Upload_Interval_Gas:
                    # sparsify data
                    filtereddf_gas = bvl_pymongodb.sparsify_data(df_gas, config.std_dict)
                    # upload sparsified data
                    filtereddf_gas.to_csv(config.csv_path, index=False)
                    bvl_pymongodb.upload_from_csv(config.dbname,config.collection_gas)#this part uploads it, but then deleted everything in the file
                    StartTimeGas = time.time() # reset timer
                    print('Gas Uploaded!')
                else:
                    continue
        
            # apply the led and email functions each scan
            if config.leds==1:
                functions.lights(s.exhaustpressure(),s.gaspressure())
            if config.email==1:
                functions.email(s.exhaustpressure(),s.gaspressure())
            
            functions.clear_buffer() # clears buffer after 1hr
            time.sleep(60*config.Interval_Between_Scans)#The time in minutes that the user chose to have between readings. Also the rate at which the uploading time increases by.
             
        except KeyboardInterrupt: #uploads everything once we close the program
            print('Closing down')
            filtereddf_exhaust = bvl_pymongodb.sparsify_data(df_exhaust, config.std_dict)
            filtereddf_exhaust.to_csv(config.csv_path, index=False)
            bvl_pymongodb.upload_from_csv(config.dbname,config.collection_exhaust)
            
            filtereddf_gas = bvl_pymongodb.sparsify_data(df_exhaust, config.std_dict)
            filtereddf_gas.to_csv(config.csv_path, index=False)
            bvl_pymongodb.upload_from_csv(config.dbname,config.collection_gas)
                   
            print('Fully uploaded!')
            break #Closes off the loop.
    
        except Exception as error:
            with open('error_log.txt','a') as error_log:# Writing to the .csv file
                error_log.write(str(time.ctime())+str(error)+'\n')# the new line is to skip a line in the csv file so that all the columns are alligned.
                error_log.close()
            print(error)
            time.sleep(5)

# by threading the loops, the GUI and database uploading will not interrupt eachother
threading.Thread(target=main_loop).start()

# Start the Tkinter event loop
m.mainloop()