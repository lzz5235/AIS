# encoding = utf-8

class dy:
    def __init__(self,obj):
        colums = ['MMSI', 'NavStatusEN', 'Draught', 'Heading', 'Course', 'HeadCourse', 'Speed', 'Rot', 'Dest',
                  'ETA', 'Receivedtime', 'UnixTime', 'Lat_d', 'Lon_d']
        if isinstance(obj,dy):
            self.__init_model(obj)
        else:
            self.__init_models(obj[0],obj[1],obj[2],obj[3],obj[4],obj[5],obj[6],obj[7],obj[8],
                               obj[9],obj[10],obj[11],obj[12],obj[13])

    def __str__(self):
        s = "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s" % \
            (self.MMSI,self.NavStatusEN,self.Draught,self.Heading,self.Course,self.HeadCourse,self.Speed,self.Rot,\
            self.Dest,self.ETA,self.Receivedtime,self.UnixTime,self.Lon_d,self.Lat_d)
        return s

    def __init_model(self,other):
        self.MMSI = other.MMSI
        self.NavStatusEN = other.NavStatusEN
        self.Draught = other.Draught
        self.Heading = other.Heading
        self.Course = other.Course
        self.HeadCourse = other.HeadCourse
        self.Speed = other.Speed
        self.Rot = other.Rot
        self.Dest = other.Dest
        self.ETA = other.ETA
        self.Receivedtime = other.Receivedtime
        self.UnixTime = other.UnixTime
        self.Lon_d = other.Lon_d
        self.Lat_d = other.Lat_d

    def __init_models(self,MMSI,NavStatusEN,Draught,Heading,Course,HeadCourse,Speed,Rot,Dest,ETA,Receivedtime,UnixTime,
                 Lat_d,Lon_d):
        self.MMSI = MMSI
        self.NavStatusEN = NavStatusEN
        self.Draught = Draught
        self.Heading = Heading
        self.Course = Course
        self.HeadCourse = HeadCourse
        self.Speed = Speed
        self.Rot = Rot
        self.Dest = Dest
        self.ETA = ETA
        self.Receivedtime = Receivedtime
        self.UnixTime = UnixTime
        self.Lat_d = Lat_d
        self.Lon_d = Lon_d

class st:
    def __init__(self,obj):
        colums = ['MMSI', 'ShipName', 'ShipTypeEN', 'Length', 'Width', 'CallSign', 'IMO']
        if isinstance(obj,st):
            self.__init_model(obj)
        else:
            self.__init_models(obj[0],obj[1],obj[2],obj[3],obj[4],obj[5],obj[6])

    def __init_model(self, other):
        self.MMSI = other.MMSI
        self.ShipName = other.ShipName
        self.ShipTypeEN = other.ShipTypeEN
        self.Length = other.Length
        self.Width = other.Width
        self.CallSign = other.CallSign
        self.IMO = other.IMO

    def __init_models(self,MMSI,ShipName,ShipTypeEN,Length,Width,CallSign,IMO):
        self.MMSI = MMSI
        self.ShipName = ShipName
        self.ShipTypeEN = ShipTypeEN
        self.Length = Length
        self.Width = Width
        self.CallSign = CallSign
        self.IMO = IMO

    def __str__(self):
        s = "%s, %s, %s, %s, %s, %s, %s" % (self.MMSI,self.ShipName,self.ShipTypeEN,self.Length,self.Width,self.CallSign,self.IMO)
        return s