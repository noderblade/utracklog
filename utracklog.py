#!/usr/bin/python3

import re
import os
import binascii
import time
import logging
import argparse
import simplekml
import socket
import socketserver


class SimpleKMLSink:
    def __init__ (self, folder, devid, rotation_time, rotation_bytes, rotation_points):
        self.filenamebase = folder + devid + "/kml/"
        self.kml = None;
        self.kml_filename = None;

        self.rotation_time = rotation_time
        self.rotation_bytes = rotation_bytes
        self.rotation_points = rotation_points

    def openfile(self):
        if self.kml:
            self.kml.save(self.filenamebase + self.kml_filename)
        del self.kml

        self.kml_filename = time.strftime("%Y_%m_%d_%H%M%S", time.localtime()) + binascii.hexlify(os.urandom(16)) + ".kml"
        self.kml = simplekml.Kml()
        self.kml.document.name = self.kml_filename;


    def start(self):
        ensure_folder(self.filenamebase)
        self.time_begin = int(time.time())
        self.time_last = self.time_begin;
        self.act_points = 0
        self.openfile()

    def dec_to_dms(self, value):
        value = float(value)
        degval = value / 100
        deg = int(degval)
        minsecs = (degval - deg) / .60
        return deg + minsecs

    def write(self, data):
        self.time_last = int(time.time())
        self.act_points += 1

        split = str(data).split(',')

        # check if communicate has at least 12 fields
        # ( minimal protocol )
        if len(split) <= 12:
            return;

        # signal lock
        signal = True if split[4] == 'F' else False

        # don't log when signal lost
        if signal == False:
            return;

        # comment or command from device
        keyword = split[1]

        # date and time
        datetime = split[2]
        latitude = split[7]
        latitude_ns = split[8]
        longitude = split[9]
        longitude_ew = split[10]

        speed = split[11]

        # in extended protocol we has sum moar info
        if len(split) > 13 and len(split) <= 18:
            altitude = split[13]
            acc = split[14]
            temperature = split[18]

        desc = "Time: %s, Keyword %s, speed %s km/h" % (datetime, keyword, speed)

        latitude_dms = self.dec_to_dms(float(latitude))
        if latitude_ns is "S":
            latitude_dms = latitude_dms * -1

        longitude_dms = self.dec_to_dms(float(longitude))
        if longitude_ew is "W":
            longitude_dms = longitude_dms * -1

        self.kml.newpoint(name=datetime, description=desc, coords=[(longitude_dms,latitude_dms)])

        flag = False
        if self.rotation_points > 0 and self.act_points > self.rotation_points:
            flag = True
        # it's ugly and uberslow - but don't see any other way right now
        if self.rotation_bytes > 0 and len(self.kml.kml(True)) > self.rotation_bytes:
            flag = True
        if self.rotation_time > 0 and ( self.time_last - self.time_begin ) >= self.rotation_time:
            flag = True

        if flag:
            self.openfile()
            self.act_points = 1
            self.time_begin = self.time_last

    def close(self):
        self.kml.save(self.filenamebase + self.kml_filename)



class RawFileSink:
    def __init__ (self, folder, devid, rotation_time, rotation_bytes, rotation_points):
        self.filenamebase = folder + devid + "/raw/"
        self.fh = None;
        self.rotation_time = rotation_time
        self.rotation_bytes = rotation_bytes
        self.rotation_points = rotation_points

    def openfile(self):
        if self.fh:
            self.fh.close()
        strfile = time.strftime("%Y_%m_%d_%H%M%S", time.localtime()) + ".log"
        self.fh = open(self.filenamebase + strfile, "a")


    def start(self):
        ensure_folder(self.filenamebase)
        self.time_begin = int(time.time())
        self.time_last = self.time_begin;
        self.act_bytes = 0
        self.act_points = 0
        self.openfile()

    def write(self, data):
        self.time_last = int(time.time())
        self.act_points += 1
        self.act_bytes += len(data)

        flag = False
        if self.rotation_points > 0 and self.act_points > self.rotation_points:
            flag = True
        if self.rotation_bytes > 0 and self.act_bytes > self.rotation_bytes:
            flag = True
        if self.rotation_time > 0 and ( self.time_last - self.time_begin ) >= self.rotation_time:
            flag = True

        if flag:
            self.openfile()
            self.act_points = 1
            self.time_begin = self.time_last
            self.act_bytes = len(data)

        self.fh.write(data+"\n")
        self.fh.flush();

    def close(self):
        self.fh.close()


class DataSink:
    # rotation time is in seconds
    def __init__ (self, folder, devid, outputs, rotation_time = 0, rotation_bytes = 0, rotation_points = 0):
        self.out = []
        for output in outputs:
            self.out.append(output(folder, devid, rotation_time, rotation_bytes, rotation_points))

    def start(self):
        for output in self.out:
            output.start()

    def write(self, data):
        outfails = 0
        for output in self.out:
            try:
                output.write(data)
            except:
                logging.error("Output %s failed for devid %s", output, devid)
                outfails = outfails + 1;

        if outfails == len(self.out):
            raise NameError("All outputs failed")

    def close(self):
        for output in self.out:
            output.close()



class TK103BHandler(socketserver.BaseRequestHandler):

        def __init__(self, request, client_address, server):
            self.READBUFF_SIZE = 4094

            self.REPLY_REGISTER_OK = bytes("LOAD\r\n", 'ascii')
            self.REPLY_PONG = bytes("ON\r\n", 'ascii')

            self.MSG_LOGIN_RE = "^##,imei:([0-9]+),.;$"
            self.MSG_NORMAL_RE = "^imei:(.*?);"
            self.MSG_HB_RE = "[0-9]*;"
            # 300 second timeout on all blocking operations
            request.settimeout(300)

            # registered imei, if empty - not registered
            self.registered = ""
            self.sink = None
            super(TK103BHandler, self).__init__(request, client_address, server)


        def handle(self):
            # device should register itself first - if it doesn't - disconnect
            # a little pink assumption here - we assume the first communicate that comes to us is single message
            try:
                data = self.request.recv(self.READBUFF_SIZE).strip().decode("ascii")
            except socket.timeout:
                logging.warning("Failed registration from %s. (timeout)" , self.client_address[0])
                return
            except:
                logging.warning("Failed registration from %s. (no data)" , self.client_address[0])
                return

            login_re = re.match(self.MSG_LOGIN_RE, data)

            if login_re:
                print(login_re.groups())
                if len(login_re.groups()) < 1:
                    return
                else:
                    self.registered = login_re.group(1);
                    logging.info("Registered %s@%s", self.registered, self.client_address[0])
                    # reply with "ok"
                    self.request.sendall(self.REPLY_REGISTER_OK)
            else:
                logging.warning("Failed registration from %s, with data %s", self.client_address[0], data)
                return

            # Create Data Sink
            self.sink = DataSink(ENV_DATA_FOLDER, self.registered, ENV_SINKS, ENV_ROTATION_TIME, ENV_ROTATION_BYTES, ENV_ROTATION_POINTS)

            try:
                self.sink.start()
            except:
                logging.error("Failed to open data sink for %s@%s", self.registered, self.client_address[0])
                return

            # reconfigure device
            # set refresh intervals
            #reqsend = "**,imei:%s,101,60s\r\n" % self.registered
            #breqsend = bytes(reqsend, "ascii")
            #self.request.sendall(breqsend)
            # time zone
            #reqsend = "**,imei:%s,108,2" % self.registered
            #breqsend = bytes(reqsend, "ascii")
            # enter normal message loop

            while data:
                try:
                    data = self.request.recv(self.READBUFF_SIZE).strip().decode("ascii")
                    logging.debug("raw data: %s", data)
                except socket.timeout:
                    logging.warning("Failed to receive data from %s. (timeout)" , self.client_address[0])
                    data = None
                    self.sink.close()
                    break
                except:
                    data = None
                    self.sink.close()
                    break

                msg_re = re.match(self.MSG_NORMAL_RE, data)

                if msg_re:
                    if len(msg_re.groups()) == 1:
                        try:
                            self.sink.write(msg_re.group(1))
                        except:
                            logging.error("Writing to sink for %s@%s failed for all outputs!", self.registered, self.client_address[0])
                            self.sink.close()
                            return;

                # send pong no matter what
                self.request.sendall(self.REPLY_PONG)

class ThreadedTCPServer(socketserver.ThreadingMixIn, socketserver.TCPServer):
    pass

# Main section

def ensure_folder(path):
    if not os.path.exists(path):
        try:
            os.makedirs(path)
        except:
            logging.error("Failed to ensure directory %s, %s", path, sys.exc_info()[0])
            exit()


if __name__ == "__main__":
    # env
    ENV_LOG_FOLDER = "./"
    ENV_DATA_FOLDER = "./DATA/"
    ENV_SINKS = [RawFileSink, SimpleKMLSink]
    ENV_ROTATION_TIME = 0
    ENV_ROTATION_BYTES = 0
    ENV_ROTATION_POINTS = 0

    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument("-l", "--log_path", help="system log path")
    parser.add_argument("-d", "--data_path", help="data log base folder path")
    parser.add_argument("-t", "--time", help="time in second on which rotate data logs", type=int)
    parser.add_argument("-b", "--bytes", help="byte limit on which rotate data logs", type=int)
    parser.add_argument("-p", "--points", help="gps points limit on which rotate data logs", type=int)
    args = parser.parse_args()

    if args.log_path:
        ENV_LOG_FOLDER = args.log_path

    if args.data_path:
        ENV_DATA_FOLDER = args.data_path

    if args.time:
        ENV_ROTATION_TIME = args.time

    if args.bytes:
        ENV_ROTATION_BYTES = args.bytes

    if args.points:
        ENV_ROTATION_POINTS = args.points

    ensure_folder(ENV_LOG_FOLDER)
    ensure_folder(ENV_DATA_FOLDER)

    # configure logger
    logging.basicConfig(format = "[%(asctime)s] %(levelname)s: %(message)s", filename = ENV_LOG_FOLDER + "utracklog.log", level=logging.DEBUG)

    logging.info("UTRACKLOG server started. \n Log path:%s\n Data path:%s", ENV_LOG_FOLDER, ENV_DATA_FOLDER)

    # start server
    HOST, PORT = "", 22000
    socketserver.TCPServer.allow_reuse_address = True
    server = ThreadedTCPServer((HOST, PORT), TK103BHandler)
    server.serve_forever()

