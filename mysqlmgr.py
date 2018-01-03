# -*- coding: utf8

import mysql.connector
import hashlib
from mysql.connector import errorcode
from meta import dy
from meta import st

class MysqlMgr:

    DB_NAME = 'AIS'

    SERVER_IP = '192.168.71.133'

    TABLES = {}
    # create new table, using sql
    TABLES['DY'] = (
        "CREATE TABLE `DY` ("
        "`idx` bigint(255) unsigned NOT NULL AUTO_INCREMENT,"
        "`MMSI` bigint(9) DEFAULT NULL,"
        "`ShipName` varchar(255) DEFAULT NULL,"
        "`NavStatusEN` varchar(255) DEFAULT NULL,"
        "`Draught` double DEFAULT NULL,"
        "`Heading` float DEFAULT NULL,"
        "`Course` float DEFAULT NULL,"
        "`HeadCourse` float DEFAULT NULL,"
        "`Speed` float DEFAULT NULL,"
        "`Rot` float DEFAULT NULL,"
        "`Dest` varchar(255) DEFAULT NULL,"
        "`ETA` datetime DEFAULT NULL,"
        "`Receivedtime` datetime DEFAULT NULL,"
        "`UnixTime` double DEFAULT NULL,"
        "`Lon_d` double NOT NULL,"
        "`Lat_d` double NOT NULL,"
        "PRIMARY KEY (`idx`)"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8")

    TABLES['ST'] = (
        "CREATE TABLE `ST` ("
        "`MMSI` bigint(9) NOT NULL,"
        "`ShipName` varchar(255) DEFAULT NULL,"
        "`ShipTypeEN` varchar(255) DEFAULT NULL,"
        "`Length` double DEFAULT NULL,"
        "`Width` double DEFAULT NULL,"
        "`CallSign` varchar(255) DEFAULT NULL,"
        "`IMO` bigint(20) DEFAULT NULL"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8")

    def __init__(self, max_num_thread):
        # connect mysql server
        try:
            cnx = mysql.connector.connect(host=self.SERVER_IP, user='root', password='123456')
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Something is wrong with your user name or password")
            elif err.errno == errorcode.ER_BAD_DB_ERROR:
                print("Database does not exist")
            else:
                print('Create Error ' + err.msg)
            exit(1)

        cursor = cnx.cursor()

        # use database, create it if not exist
        try:
            cnx.database = self.DB_NAME
        except mysql.connector.Error as err:
            if err.errno == errorcode.ER_BAD_DB_ERROR:
                pass
            else:
                print(err)
                exit(1)
        finally:
            cursor.close()
            cnx.close()

        dbconfig = {
            "database": self.DB_NAME,
            "user":     "root",
            "password": "qwerty12",
            "host":     self.SERVER_IP,
        }
        self.cnxpool = mysql.connector.connect(pool_name="mypool",
                                                          pool_size=max_num_thread,
                                                          **dbconfig)
    # create databse
    def create_database(self, cursor):
        try:
            cursor.execute(
                "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(self.DB_NAME))
        except mysql.connector.Error as err:
            print("Failed creating database: {}".format(err))
            exit(1)

    def create_tables(self, cursor):
        for name, ddl in self.TABLES.items():
            try:
                cursor.execute(ddl)
            except mysql.connector.Error as err:
                if err.errno == errorcode.ER_TABLE_EXISTS_ERROR:
                    print('create tables error ALREADY EXISTS')
                else:
                    print('create tables error ' + err.msg)
            else:
                print('Tables created')

    def enqueue_DY(self, d):
        con = mysql.connector.connect(pool_name="mypool")
        cursor = con.cursor()
        try:
            add_dy = ("INSERT INTO DY (MMSI, NavStatusEN, Draught, Heading, Course, HeadCourse, Speed, "
                          "Rot, Dest, ETA, Receivedtime, UnixTime, Lon_d, Lat_d"
                          ") VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
            data_dy = ( d.MMSI, str(d.NavStatusEN), d.Draught, d.Heading, d.Course, d.HeadCourse, d.Speed,
                             d.Rot, str(d.Dest), d.ETA.strftime('%Y-%m-%d %H:%M:%S'), d.Receivedtime.strftime(
                                '%Y-%m-%d %H:%M:%S'),int(d.UnixTime),d.Lon_d, d.Lat_d)

            cursor.execute(add_dy, data_dy)
                # commit this transaction, please refer to "mysql transaction" for more info
            con.commit()
        except mysql.connector.Error as err:
            print 'enqueueDY() ' + err.msg
            return
        finally:
            cursor.close()
            con.close()

    def fetch_DY(self,MMSI):
        con = mysql.connector.connect(pool_name="mypool")
        cursor = con.cursor()
        try:
            query = ("SELECT * FROM DY WHERE `MMSI`=%s") % (MMSI)
            cursor.execute(query)
            if cursor.rowcount is 0:
                return None
            rows = cursor.fetchall()
            alldys = [dy(r) for r in rows]
            return alldys
        except mysql.connector.Error as err:
            print 'all_DY() ' + err.msg
            return None
        finally:
            cursor.close()
            con.close()

    def fetch_ST(self,MMSI):
        con = mysql.connector.connect(pool_name="mypool")
        cursor = con.cursor()
        try:
            query = ("SELECT * FROM ST WHERE `MMSI`=%s") % (MMSI)
            cursor.execute(query)
            if cursor.rowcount is 0:
                return None
            rows = cursor.fetchall()
            allsts = [st(r) for r in rows]
            return allsts
        except mysql.connector.Error as err:
            print 'all_DY() ' + err.msg
            return None
        finally:
            cursor.close()
            con.close()

    def enqueue_ST(self, s):
        con = mysql.connector.connect(pool_name="mypool")
        cursor = con.cursor()
        try:
            add_ST = ("INSERT INTO ST (MMSI, ShipName, ShipTypeEN, Length, Width, CallSign, IMO) VALUES (%s, %s, %s, "
                     "%s, %s, %s, %s)")
            data_ST = (s.MMSI,s.ShipName,s.ShipTypeEN,s.Length,s.Width,s.CallSign,s.IMO)
            cursor.execute(add_ST, data_ST)
            # commit this transaction, please refer to "mysql transaction" for more info
            con.commit()
        except mysql.connector.Error as err:
            print 'enqueue_ST() ' + err.msg
            # return
        finally:
            cursor.close()
            con.close()

    # # get an url from queue
    # def dequeue_ST(self, biz):
    #     con = mysql.connector.connect(pool_name="mypool")
    #     cursor = con.cursor(dictionary=True)
    #     try:
    #         # use select * for update to lock the rows for read
    #         query = ("SELECT `index`, `url`, `biz` FROM urls WHERE status='new' AND biz='" + biz + "' ORDER BY `index` ASC LIMIT 1 FOR UPDATE")
    #         cursor.execute(query)
    #         if cursor.rowcount is 0:
    #             return None
    #         row = cursor.fetchone()
    #         update_query = ("UPDATE urls SET `status`='downloading' WHERE `index`=%d") % (row['index'])
    #         cursor.execute(update_query)
    #         con.commit()
    #         return row
    #     except mysql.connector.Error as err:
    #         # print 'dequeueUrl() ' + err.msg
    #         return None
    #     finally:
    #         cursor.close()
    #         con.close()

    # def finish_url(self, index):
    #     con = mysql.connector.connect(pool_name="mypool")
    #     cursor = con.cursor()
    #     try:
    #         # we don't need to update done_time using time.strftime('%Y-%m-%d %H:%M:%S') as it's auto updated
    #         update_query = ("UPDATE urls SET `status`='done' WHERE `index`=%d") % (index)
    #         cursor.execute(update_query)
    #         con.commit()
    #     except mysql.connector.Error as err:
    #         # print 'finishUrl() ' + err.msg
    #         return
    #     finally:
    #         cursor.close()
    #         con.close()
