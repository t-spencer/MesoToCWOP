__author__ = "Trent Spencer, Texas A&M University"
__version__ = "1.2"


# Change these to your values, you will also likely have to edit the variable names (such as RH for humidity
# or AT for the Temperature) in the below code

CWOPid = "FW####"
File = 'Mesonet.dat'
Lat = '####.##N'
Lon = '#####.##W'
StationHeight = 67 # in M
#----------------------------------------------------------------------------------

# Imports necessary modules
import pandas as pd
import subprocess as sp
import numpy as np
import socket
import schedule
import time

# Unit Conversion Functions
def mps_to_mph(WindSpeed):
	return WindSpeed * 2.23694
	
def Cel_to_F(Temperature):
	return ((9.0/5.0) * Temperature) + 32

def AltimeterAdjust(Press):
	Height = StationHeight
	P1 = Press - 0.3
	Frac1 = ((1013.25**0.19284) * 0.0065) / 288
	Frac2 = Height / ((Press-0.3)**(0.190284))
	P2 = (1 + (Frac1*Frac2))**(1/0.190284)
	return P1*P2
		
# Definition of Operational Code
class MesoToCWOP:
	def __init__(self, file):
		self.filename = file
		
	def GetLastData(self):
		'''Retrieves and saves the last line of data'''
		File = self.filename
		self.LastData = pd.read_csv(File, skiprows=[0,2,3], parse_dates=[0]).tail(n=1)
		
	def FormatData(self):
		'''Gets the data into the required CWOP format'''
		id = CWOPid + ">APRS,TCPIP*:"
		timedata = self.LastData['TIMESTAMP'].values[0]
		time = pd.to_datetime(timedata)
		Time = '@' + "{:0>2d}".format(time.day) + "{:0>2d}".format(time.hour) + "{:0>2d}".format(time.minute) + 'z' 
		LatLon = Lat + "/" + Lon
		WinD = "_" + "{:0>3d}".format(int(round(self.LastData['WD'].values[0]))) 
		WinS = "/" + "{:0>3d}".format(int(round(mps_to_mph(self.LastData['WS'].values[0])))) 
		Gust = "g..." #+ "{:0>3d}".format(int(round(mps_to_mph(self.LastData['WSgst'].values[0]))))
		if self.LastData['AT'].values[0] < 0:
			Temp = "{:.0f}".format(round(Cel_to_F(self.LastData['AT'].values[0])))
			RepTemp = "t-" + "{:0>2d}".format(int(Temp))
		if self.LastData['AT'].values[0] > 0:
			Temp = "{:.0f}".format(round(Cel_to_F(self.LastData['AT'].values[0]))) 
			RepTemp = "t" + "{:0>3d}".format(int(Temp))		
		RainfallHour = 'r' +  "{:0>3d}".format(int(round(self.LastData['RN60'].values[0] * 100)))
		# Rainfall24Hour = 'p...' 
		RainfallDaily = "P" + "{:0>3d}".format(int(round(self.LastData['RNDAY'].values[0] * 100)))
		Pressure = "b" + "{:0>5d}".format(int(round((AltimeterAdjust(self.LastData['BP'].values[0])) * 10)))
		if self.LastData['RH'].values[0] == 100:
			Humidity = "h00"
		else:
			Humidity = "h" + "{:0>2d}".format(int(round(self.LastData['RH'].values[0])))
		M2 = id + Time + LatLon + WinD + WinS + Gust + RepTemp + RainfallHour + RainfallDaily + Humidity + Pressure + "\r\n"
		self.Message2 = M2.encode('utf-8')
		
	def SendData(self):
		'''Sends the formatted data to the CWOP servers'''
		TCP_IP = 'cwop.aprs.net'
		TCP_PORT = 14580 # Options: 23/14580
		BUFFER_SIZE = 1024
		Log = 'user ' + CWOPid+ ' pass -1 vers MesoToCWOP.py 1.2\r\n'
		Login = Log.encode('utf-8')
		Message = self.Message2
		sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
		sock.connect((TCP_IP, TCP_PORT))
		Return1 = sock.recv(BUFFER_SIZE)
		sock.send(Login)
		Return2 = sock.recv(BUFFER_SIZE)
		sock.send(Message)
		sock.close()

def Job():
	'''Defines the job to be scheduled'''
	Mesonet = MesoToCWOP(File)
	Mesonet.GetLastData()
	Mesonet.FormatData()
	Mesonet.SendData()
	
schedule.every(5).minutes.do(Job) # Schedules the above job

# Running the scheduled jobs (i.e., the actual running of the code)
while True:
	schedule.run_pending()
	time.sleep(1)
