# encoding = utf-8

import os
import xml.etree.ElementTree as ET
import xml.dom.minidom as minidom
import numpy as np
import pandas as pd
from pandas import datetime as pdatetime
import datetime
import time
from meta import dy
from meta import st


class parse:
    def __init__(self,input_file_path,input_folder_path=""):
        self.input_file_path = input_file_path
        self.input_folder_path = input_folder_path
        self.all_MMSI_DY = []
        self.all_MMSI_ST = []

    def get_data_DY_gen(self):
        data_paths_dy = []
        folder = self.input_folder_path + os.sep + 'DY'
        for file in os.listdir(folder):
            tmp_dy = [os.path.join(folder + os.sep + file, s) for s in os.listdir(folder + os.sep + file)]
            data_paths_dy += (tmp_dy)

        for file in data_paths_dy:
            if 0 == int(os.path.getsize(file)):
                continue
            print file
            with open(file,'r+') as fd:
                yield self.get_data_DY(fd)

    def get_data_ST_gen(self):
        data_paths_st = []
        folder = self.input_folder_path + os.sep + 'ST'
        for file in os.listdir(folder):
            tmp_st = [os.path.join(folder + os.sep + file, s) for s in os.listdir(folder + os.sep + file)]
            data_paths_st += (tmp_st)

        for file in data_paths_st:
            if 0 == int(os.path.getsize(file)):
                continue
            with open(file,'r+') as fd:
                yield self.get_data_ST(fd)

    def get_data_DY(self,fd):
        alldys = []

        try:
            et = ET.parse(fd)
        except ET.ParseError,e:
            print e
            return alldys

        element = et.getroot()
        element_Ships = element.findall('Ship')
        for ship in element_Ships:
            mmsi = long(ship.find("MMSI").text)
            DynamicInfo = ship.find("DynamicInfo")
            LastTime = DynamicInfo.find("LastTime").text
            Latitude = float(DynamicInfo.find("Latitude").text)
            Longitude = float(DynamicInfo.find("Longitude").text)
            Speed = float(DynamicInfo.find("Speed").text)
            course = float(DynamicInfo.find("course").text)
            HeadCourse = float(DynamicInfo.find("HeadCourse").text)
            AngularRate = float(DynamicInfo.find("AngularRate").text)
            NaviStatus = float(DynamicInfo.find("NaviStatus").text)
            # ShipData = {'MMSI':mmsi, 'DynamicInfo':[]}
            # ShipData['DynamicInfo'].append({'LastTime':str(LastTime),'Latitude':Latitude,'Longitude':Longitude,
            #                                 'Speed':Speed,
            #                                 'course':course,'HeadCourse':HeadCourse,'AngularRate':AngularRate,
            #                                 'NaviStatus':NaviStatus})

            obj = [mmsi,NaviStatus,0,course,course,HeadCourse,Speed,AngularRate,"",
                   pdatetime.strptime(LastTime,'%Y-%m-%d %H:%M:%S'),pdatetime.strptime(LastTime, '%Y-%m-%d %H:%M:%S'),
                   time.mktime(pdatetime.strptime(LastTime, '%Y-%m-%d %H:%M:%S').timetuple()),float(Latitude),float(Longitude)]
            d = dy(obj)

            if mmsi < 100000000:
                continue

            alldys.append(d)
        return alldys

    def get_data_ST(self,fd):
        allsts = []

        try:
            et = ET.parse(fd)
        except ET.ParseError,e:
            print e
            return allsts

        element = et.getroot()
        element_Ships = element.findall('Ship')
        for ship in element_Ships:
            mmsi = long(ship.find("MMSI").text)
            StaticInfo = ship.find("StaticInfo")
            LastTime = StaticInfo.find("LastTime").text
            ShipType = int(StaticInfo.find("ShipType").text)
            Length = float(StaticInfo.find("Length").text)
            Width = float(StaticInfo.find("Width").text)
            Left = float(StaticInfo.find("Left").text)
            Trail = float(StaticInfo.find("Trail").text)
            Draught = float(StaticInfo.find("Draught").text)
            IMO = long(StaticInfo.find("IMO").text)
            CallSign = StaticInfo.find("CallSign").text
            ETA = StaticInfo.find("ETA").text
            Name = StaticInfo.find("Name").text
            Dest = StaticInfo.find("Dest").text
            # ShipData = {'MMSI': mmsi, 'StaticInfo': []}
            # ShipData['StaticInfo'].append({'LastTime': str(LastTime), 'ShipType': ShipType, 'Length': Length,
            #                                'Width': Width,
            #                                'Left': Left, 'Trail': Trail, 'Draught': Draught,
            #                                'IMO': IMO,
            #                                'CallSign': str(CallSign), 'ETA': str(ETA), 'Name': str(Name),
            #                                'Dest': str(Dest)})
            obj = [mmsi,Name,ShipType,Length,Width,CallSign,IMO]
            s = st(obj)

            if mmsi < 100000000:
                continue

            allsts.append(s)

        return allsts

    def segment_table_by_dest(self):
        split = []
        for idx in range(len(self.table) - 1):
            if str(self.table.iloc[idx]['Dest']) != str(self.table.iloc[idx + 1]['Dest']):
                split.append(idx)

        for idx in range(len(split)-1):
            tmp = self.table[split[idx]+1:split[idx+1]-1]
            self.seg_tab.append(tmp)

    def read_data_txt_dy_gen(self):
        def conv_map(Longitude):
                if Longitude <= 0:
                    Longitude += 360  # correct longtitude
                return Longitude

        colums = np.array(
            ["ShipName", "CallSign", "IMO", "MMSI", "ShipTypeCN", "ShipTypeEN", "NavStatusCN", "NavStatusEN", "Length",
             "Width", "Draught", "Heading", "Course", "Speed", "Lon", "Lat", "Rot", "Dest", "ETA", "Receivedtime",
             "UnixTime",
             "Lon_d", "Lat_d"])

        files = [os.path.join(self.input_folder_path, file) for file in os.listdir(self.input_folder_path)]
        for file in files:
            table = pd.read_csv(file,sep=',',names=colums,header=0,encoding='GB2312')
            for ix,row in table.iterrows():
                mmsi = row['MMSI']
                LastTime = row['Receivedtime']
                Latitude = row['Lat_d']
                Longitude = conv_map(row['Lon_d'])
                Speed = row['Speed']
                course = row['Course']
                HeadCourse = row['Course']
                AngularRate = row['Rot']
                NaviStatus = row['NavStatusEN']
                Draught = row['Draught']
                Heading = row['Heading']
                Dest = row['Dest']
                ETA = row['ETA']

                obj = [mmsi, NaviStatus, Draught, Heading, course, HeadCourse, Speed, AngularRate, Dest,ETA,
                       pdatetime.strptime(LastTime, '%Y-%m-%d %H:%M:%S'),
                       time.mktime(pdatetime.strptime(LastTime, '%Y-%m-%d %H:%M:%S').timetuple()), float(Latitude),
                       float(Longitude)]
                d = dy(obj)

                if mmsi < 100000000:
                    continue

                yield d

    def read_data_txt_st_gen(self):
        colums = np.array(["MMSI", "ShipName", "ShipTypeEN", "Length", "Width", "CallSign", "IMO"])

        files = [os.path.join(self.input_folder_path, file) for file in os.listdir(self.input_folder_path)]
        for file in files:
            table = pd.read_csv(file,sep=',',names=colums,header=0,encoding='GB2312')
            for ix,row in table.iterrows():
                mmsi = row['MMSI']
                ShipName = row['ShipName']
                ShipTypeEN = row['ShipTypeEN']
                Length = row['Length']
                Width = row['Width']
                CallSign = row['CallSign']
                IMO = row['IMO']

                obj = [mmsi, ShipName, ShipTypeEN, Length, Width, CallSign, IMO]
                s = st(obj)

                if mmsi < 100000000:
                    continue

                yield s

    def get_data_DY_cxw_gen(self):
        data_paths_dy = []
        folder = self.input_folder_path + os.sep + 'DY'
        for file in os.listdir(folder):
            tmp_dy = [os.path.join(folder + os.sep + file, s) for s in os.listdir(folder + os.sep + file)]
            data_paths_dy += (tmp_dy)

        for file in data_paths_dy:
            if 0 == int(os.path.getsize(file)):
                continue
            print file
            with open(file,'r+') as fd:
                yield self.get_data_cxw_DY(fd)

    def get_data_cxw_DY(self,fd):
        alldys = []

        try:
            et = ET.parse(fd)
        except ET.ParseError,e:
            print e
            return alldys

        element = et.getroot()
        element_Ships = element.findall('Ship')
        for ship in element_Ships:
            mmsi = long(ship.find("MMSI").text)
            DynamicInfo = ship.find("DynamicInfo")
            LastTime = DynamicInfo.find("PositionTime").text
            Latitude = float(DynamicInfo.find("Latitude").text)
            Longitude = float(DynamicInfo.find("Longitude").text)
            Speed = float(DynamicInfo.find("Speed").text)
            course = float(DynamicInfo.find("Course").text)
            HeadCourse = float(0)
            AngularRate = float(0)
            NaviStatus = float(DynamicInfo.find("Status").text)

            try:
                obj = [mmsi,NaviStatus,0,course,course,HeadCourse,Speed,AngularRate,"",
                       pdatetime.strptime(LastTime,'%Y-%m-%d %H:%M'),pdatetime.strptime(LastTime, '%Y-%m-%d %H:%M'),
                       time.mktime(pdatetime.strptime(LastTime, '%Y-%m-%d %H:%M').timetuple()),float(Latitude),
                       float(Longitude)]
                d = dy(obj)

            except ValueError as e:
                print e
                continue

            if mmsi < 100000000:
                continue

            alldys.append(d)

        return alldys