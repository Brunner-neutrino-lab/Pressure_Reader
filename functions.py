# functions for LEDs and sensors
import RPi.GPIO as GPIO         # Import Raspberry Pi GPIO library
from time import sleep          # Import the sleep function 
import sys 

sys.path.append('BvL-MongoDB')
import bvl_pymongodb

# for uploadng to database
import bvl_pymongodb
from datetime import datetime # to get the current time

#for database stuff
import sensors as s # for sensor data reading

# for emailing
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

# for GUI
import tkinter as tk # to display data in a seperate window
import threading
import time


def lights(e, g):
    LEDRED = 18  # LED GPIO Pin LED
    LEDGREEN = 17

    GPIO.setmode(GPIO.BCM)          # Use GPIO pin number
    GPIO.setwarnings(False)         # Ignore warnings in our case
    GPIO.setup(LEDRED, GPIO.OUT)    # GPIO pin as output pin
    GPIO.setup(LEDGREEN, GPIO.OUT)

    if g > 100 or g < 0:
        GPIO.output(LEDRED, GPIO.HIGH)  # Turn on RED LED
        print('Gas Pressure Out of Range')
    else:
        GPIO.output(LEDRED, GPIO.LOW)   # Turn off RED LED

    if e > 30 or e < -14.7:
        GPIO.output(LEDGREEN, GPIO.HIGH)  # Turn on GREEN LED
        print('Exhaust Pressure Out of Range')
    else:
        GPIO.output(LEDGREEN, GPIO.LOW)   # Turn off GREEN LED



# header for correct formatting in .csv file for database
# gives names for each column and units
# since exhaust and gas pressures are not being uploaded at the same intervals, two seperate files are created

def Header_Exhaust(): #The header used when uploading the data to the csv file. 
    if bvl_pymongodb.cfg.ExhaustStatus == 1:
        statement='timestamp', 'Exhaust_Pressure_psig'
    else:
        statement = 'timestamp'
    return statement

def Header_Gas():
    if bvl_pymongodb.cfg.GasStatus == 1:
        statement ='timestamp, Gas_Pressure_psig'
    else:
        statement = 'timestamp'
    return statement

def Header_All():
    if bvl_pymongodb.cfg.ExhaustStatus == 1 and bvl_pymongodb.cfg.GasStatus == 1:
        statement='timestamp, Exhaust_Pressure_psig, Gas_Pressure_psig' #by default we'll always have the time. 
    return statement

#This is how all the data is collected and stringed together. It is properly formated for the bvl-MongoDB script. 
def data_exhaust():#The data readings from the sensor. 
    if bvl_pymongodb.cfg.ExhaustStatus==1:
        statement = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) + str(s.exhaustpressure())
    else:
        statement = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return statement

def data_gas():#The data readings from the sensor. 
    if bvl_pymongodb.cfg.GasStatus==1:
        statement = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) +str(s.gaspressure())
    else:
        statement=str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return statement

def data_all():
    if bvl_pymongodb.cfg.ExhaustStatus==1 and bvl_pymongodb.cfg.GasStatus==1:
        statement = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")) +str(s.exhaustpressure())
    else:
        statement = str(datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    return statement

# sending notifying email

# Pressure sensor reading and checking logic
# exhaust pressure threshold: -14.7 to 30 psig
# gas pressure threshold: 0 to 100 psig

def send_email(recipient_email, message):
    sender_email = "Pressure_sensor_alerts@outlook.com"
    sender_password = "C00lpre$$ure$sens0r"
    subject = "Pressure Sensor Alert"

    msg = MIMEText(message)
    msg["Subject"] = subject
    msg["From"] = sender_email
    msg["To"] = recipient_email

    with smtplib.SMTP("smtp-mail.outlook.com", 587) as server:
        server.starttls()
        server.login(sender_email, sender_password)
        server.sendmail(sender_email, [recipient_email], msg.as_string())

def email(e, g):
    if e > 30 or e < -14.7:
        message = f"Exhaust Pressure is out of range: {e} psig"
        send_email("grace.champagne@mail.mcgill.ca", message)
    if g > 100 or g < 0:
        message = f"Compressed Gas Pressure is out of range: {g} psig"
        send_email("grace.champagne@mail.mcgill.ca", message)
    else:
        print("All good!")

# for GUI
# Function to update values every second
def update_values(exh_lbl, compr_gas_lbl, m, stopped):
    # Update exhaust and gas values here (example: random values)
    exhaust_value = s.exhaustpressure()
    gas_value = s.gaspressure()
    
    # Update labels with the new values
    exh_lbl.config(text="{:.2f}".format(exhaust_value)) # these values are defined in main program
    compr_gas_lbl.config(text="{:.2f}".format(gas_value))
    
    # Update happy/sad faces based on pressure range
    # update_face(exhaust_value, gas_value)
    
    # Schedule the next update after 1000 milliseconds (1 second)
    if not stopped:
        m.after(10000, update_values,exh_lbl, compr_gas_lbl, m, stopped) # m is the tkinter window defined in the main program

# Function to update happy/sad faces based on pressure range
def update_face(exhaust_value, gas_value):
    if -14.7 <= exhaust_value <= 30:
        psig_face_label.config(text="😊")  # Happy face
    else:
        psig_face_label.config(text="☹️")  # Sad face
    
    if 0 <= gas_value <= 100:
        compressed_gas_face_label.config(text="😊")  # Happy face
    else:
        compressed_gas_face_label.config(text="☹️")  # Sad face

        
# clear buffer frame
# Define the maximum buffer duration in seconds (1 hour)
MAX_BUFFER_DURATION = 3600

# Function to clear the bufferDataFrame after 1 hour (tested and works)
def clear_buffer(bufferDataFrame):
    if len(bufferDataFrame) > 0:
        # Get the timestamp of the earliest entry in the bufferDataFrame
        earliest_timestamp = bufferDataFrame['timestamp'].min()
        # Get the current time
        current_time = datetime.datetime.now()
        # Calculate the time difference in seconds
        time_diff = (current_time - earliest_timestamp).total_seconds()
        # If the time difference exceeds one hour, truncate the bufferDataFrame
        if time_diff >= MAX_BUFFER_DURATION:
            # Remove entries older than 1 hour
            bufferDataFrame = bufferDataFrame[bufferDataFrame['timestamp'] >= (current_time - datetime.timedelta(seconds=MAX_BUFFER_DURATION))]
    return bufferDataFrame
