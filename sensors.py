# Author: Grace Champagne
# Date: Nov 23, 2023

# main program to read, log and display pressure values of exhaust and compressed gas

import time
# board and busio are from adafruit circuit python
import board
import busio
import adafruit_ads1x15.ads1115 as ADS
from adafruit_ads1x15.analog_in import AnalogIn
import functions #for lights

# create i2c bus
i2c = busio.I2C(board.SCL, board.SDA)

# create ADC object using i2c bus
ads1 = ADS.ADS1115(i2c, address = int(0x48)) 


# connected to channel 0 (A0 pin)
chan1 = AnalogIn(ads1, ADS.P0)
chan2 = AnalogIn(ads2, ADS.P1)


# function for sensor data

# exhaust sensor outputs 1-5V but scaled down to 3.3V
# multiply by 5/3.3
scaling = 5/3.3
def exhaustpressure():
    exhaust = ((chan1.voltage*scaling)*11.175 - 11.175) 
    return exhaust

# compressed gas sensor outputs 0-5V
# multiply by 5/3.3
def gaspressure():
    gas = ((chan2.voltage*scaling)*20)
    return gas
