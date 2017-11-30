#!/usr/bin/env python



"""
Basic structure: 666+id+data

Existing commands:

id | data                                                            | meaning
—--|-----------------------------------------------------------------|------—
1  | node_token[128] ip[4] port[2]                                   | handshake
2  | token[128] filename[128]                                        | client to storage - get file chunk request
3  | filename[128] total[2] number[2] datasize[2] data               | storage to client - send file chunk
4  | token[128] filename[128]                                        | storage to ns - check client-file permissions
5  | token[128] filename[128] T/F[1]                                 | ns to storage - check result
6  | error_code[1]                                                   | for different errors
7  | size[1] login size[1] pass                                      | client to ns - auth
8  | token[128]                                                      | ns to client - auth
9  | token[128]                                                      | client to ns - request the tree
10 | total[1] number[1] datasize[2] data                             | ns to client - send the tree
11 | token[128] size[2] filepath                                     | client to ns - request file info   (not required)
12 | total[1] number[1] datasize[2] data                             | ns to client - send file info      (not required)
13 | token[128] size[2] filepath                                     | client to ns - get file request
14 | total[1] number[1] datasize[2] data                             | ns to client - send file's chunks locations
15 | token[128] size[2] filepath total[2] number[2] datasize[2] data | client to ns - update file request
16 | size[2] filepath T/F[1]                                         | ns to client - file update result
17 | token[128] size[2] filepath                                     | client to ns - file delete request
18 | size[2] filepath T/F[1]                                         | ns to client - file delete result
19 | token[128] size[2] srcfilepath size[2] dstfilepath              | client to ns - rename file request
20 | size[2] srcfilepath size[2] dstfilepath T/F[1]                  | ns to client - rename file result
21 | filename[128] total[2] number[2] datasize[2] data               | ns to storage - send file chunk to storage ((????draft????__
22 | filename[128] T/F[1]                                            | storage to ns - file save result
23 | filename[128]                                                   | ns to storage - delete file
24 | filename[128] T/F[1]                                            | storage to ns - file delete result
25 | filename[128] total[2] number[2] datasize[2] data               | ns to storage - update file, draft
26 | filename[128] T/F[1]                                            | storage to ns - file update, draft, result
27 | filename[128] T/F[1]                                            | ns to storage - update file
28 | filename[128] T/F[1]                                            | storage to ns - update file result
29 |                                                                 | ns to storage - get memory information
30 | size[8] total size[8] busy                                      | storage to ns - send memory information
"""



from __future__ import with_statement

import string
import os
import sys
import errno
import time
import getpass
import socket                  
import struct
import json
from threading import Thread
import signal
import shutil
import logging

from fuse import FUSE, FuseOSError, Operations
from stat import S_IREAD, S_IRGRP, S_IROTH, S_IWUSR




# Name Server
NS = {"IP":"188.130.155.43", "port": 9091 }
# Directories
ROOT = "/tmp/fuse"
MOUNTPOINT = "DevilStore"
DEBUG = False
LOGS = False


# Global TEMP
LIST_FILES = {}
LIST = []
TOTAL_SIZE = 0
TOTAL_SIZE_VIR = 0
TOKEN = b''
TREE = []
SYSTEM_INFO = {}
LIVE = False
KEEP_ALIVE_GL = object


class bc:
    HEADER =    '\033[95m'     #
    OKBLUE =    '\033[94m'
    OKGREEN =   '\033[92m'
    WARNING =   '\033[93m'
    FAIL =      '\033[91m'
    ENDC =      '\033[0m'
    BOLD =      '\033[1m'
    UNDERLINE = '\033[4m'
    RED   =     "\033[1;31m"  
    BLUE  =     "\033[1;34m"
    CYAN  =     "\033[1;36m"
    GREEN =     "\033[0;32m"
    RESET =     "\033[0;0m"
    BOLD    =   "\033[;1m"
    REVERSE =   "\033[;7m"
    def red(stroka):
        return bc.RED + stroka + bc.ENDC
    def green(stroka):
        return bc.GREEN + stroka + bc.ENDC
    def cyan(stroka):
        return bc.CYAN + stroka + bc.ENDC
    def waring(stroka):
        return bc.WARNING + stroka + bc.ENDC

def packets(id, data=None, total=0, number=0):
    global TOKEN
    sep=666
    if   id==2:     #token[128] filename[64]                                      | client to storage - get file chunk request
        
        box = struct.pack('<hB128s64s', sep, id, 
            TOKEN, data.encode('utf-8'))
        #print(struct.unpack('<hB128s64s',box))
    
    elif id==3:     #filename[64] total[2] number[2] datasize[2] data             | storage to client - send file chunk
        
        pass

    elif id==6:     #error_code[1]                                                 | for different errors
        
        pass # Error
        # 1- Flash AOSP or LOS Base Rom
        # 2- Reboot phone setup wizard finish
        # 3- Reboot Recovery
        # 4- Flash Nethunter Zip
        # 5- Reboot

    elif id==7:     #size[1] login size[1] pass                                    | client to ns - auth
        
        ld0 = len(data[0])
        ld1 = len(data[1])
        box = struct.pack('<hBB'+str(ld0)+'sB'+str(ld1)+'s', sep, id, 
            ld0, data[0].encode('utf-8'), ld1, data[1].encode('utf-8'))
        # print(struct.unpack('<hBB'+str(ld0)+'sB'+str(ld1)+'s', box))

    elif id==8:     #token[128]                                                    | ns to client - auth
        
        box = struct.unpack('<128s',data)[0]

    elif id==9:     #token[128]                                                    | client to ns - request the tree
        
        box = struct.pack('<hB128s',sep,id, data)
    
    elif id==10:     #total[1] number[1] datasize[2] data                          | ns to client - send the tree
        
        pass
    
    elif id==11:     #token[128] size[2] filepath                                  | client to ns - request file info   (not required)
        
        pass

    elif id==12:     #total[1] number[1] datasize[2] data                          | ns to client - send file info (not required)
        
        pass

    elif id==13:     #token[128] size[2] filepath                                  | client to ns - get file request
        
        box = struct.pack('<hB128sh'+str(len(data))+'s', sep, id, 
            TOKEN, len(data), data.encode('utf-8'))
    
    elif id==14:     #total[1] number[1] datasize[2] data                          | ns to client - send file's chunks locations
        
        pass 
    
    elif id==15:     #token[128] size[2] filepath *total[2] *number[2] datasize[2] data | client to ns - update file request
        
        box = struct.pack('<hB128sh'+str(len(data[0]))+'sh'+str(len(data[1]))+'s', sep, id,
            TOKEN, len(data[0]), data[0].encode('utf-8'),  len(data[1]), data[1].encode('utf-8'))
    
    elif id==16:     #total[1] number[1] datasize[2] data                          | ns to client - file update result
        
        pass

    elif id==17:     #token[128] size[2] filepath                                  | client to ns - file delete request
        
        box = struct.pack('<hB128sh'+str(len(data))+'s', sep, id, 
            TOKEN, len(data), data.encode('utf-8'))
    
    elif id==18:     #size[2] filepath T/F[1]                                      | ns to client - file delete result
        
        pass
    
    elif id==19:     #token[128] size[2] srcfilepath size[2] dstfilepath           | client to ns - rename file request
        
        box = struct.pack('<hB128sh'+str(len(data[0]))+'sh'+str(len(data[1]))+'s', sep, id,
            TOKEN, len(data[0]), data[0].encode('utf-8'), len(data[1]), data[1].encode('utf-8'))
    
    elif id==20:     #size[2] srcfilepath size[2] dstfilepath T/F[1]                 | ns to client - rename file result
        
        pass

    elif id==21:     #filename total[2] number[2] datasize[2] data                   | client to ns - update file request
    
        box = struct.pack('<hB64shhh'+str(len(data[1]))+'s', sep, id,
            data[0].encode('utf-8'), total, number,  len(data[1]), data[1])
        #print( struct.unpack('<hB64shhh'+str(len(data[1]))+'s', box))

    elif id==34:     #TOKEN                                                          | client to ns - keep alive 
        
        box = struct.pack('<hB128s',sep,id, TOKEN)

    elif id==35:     #TOKEN filename                                                 | client to storage ask
        
        box = struct.pack('<hB128s64s', sep, id, 
            TOKEN, data.encode('utf-8'))

    elif id==36:     #filename[64] T/F[1]                                                 | storage to client response
    
        pass

    elif id==37:     #TOKEN                                                 | client exit

        box = struct.pack('<hB128s', sep, id, 
            TOKEN)
    elif id==38:     ## TOKEN  total[1] number[1] datasize[2] data  

        box = struct.pack('<hB128sBBh' + str(len(data)) + 's', sep, id, 
            TOKEN, total, number, len(data), data.encode('utf-8'))    
    
    return box

def GL_exit():
    global LIVE, NS
    ns_host = NS["IP"] 
    ns_port = NS["port"]
    s = socket.socket()
    try:
        s.settimeout(10)                    
        s.connect((ns_host, ns_port))
        mes = packets(37)
        s.send(mes)
        s.close()
    except (ConnectionRefusedError,  socket.timeout) as e:
        pass
    print("\nExiting ...")
    logs.info("\nExiting ...")
    LIVE = False 
    # KEEP_ALIVE_GL.join() if KEEP_ALIVE_GL else 0
    try:
        sys.exit()
    except:
        pass

def keep_alive():
    global NS, LIVE, LOGS
    ns_host = NS["IP"] 
    ns_port = NS["port"] 
    while LIVE:
        for i in range(10):
            time.sleep(1)
            if not LIVE:
                return
        mes = packets(34)
        s = socket.socket()
        s.settimeout(5)  
        try:                  
            s.connect((ns_host, ns_port))
        except:
            print(bc.red("Keep Alive: Connectiom with NS lost"))
            logs.error("Keep Alive: Connectiom with NS lost") if LOGS else 0
            GL_exit()
        s.send(mes)
        s.close()

def generate_info(path):
    global ROOT

    stat = os.lstat(ROOT + "/" + path)
    if os.path.isfile(ROOT + "/" + path): 
        info = {"accessed": stat.st_atime , "created": stat.st_ctime, "modified": stat.st_mtime, "filesize": stat.st_size, "filepath": path}
    elif os.path.isdir(ROOT + "/" + path):
        info = {"accessed": stat.st_atime , "created": stat.st_ctime, "modified": stat.st_mtime, "filesize": 0, "filepath": path + "/"}
    return info

def get_tree_size(path):
    """Return total size of files and create list of files."""
    global LIST_FILES
    total = 0
    for entry in os.scandir(path):
        if entry.is_dir(follow_symlinks=False):
            if ".Trash-" not in entry.path:
                LIST_FILES.update({entry.path + "/" : entry.stat(follow_symlinks=False).st_size})
                total += get_tree_size(entry.path)
        else:
            LIST_FILES.update({entry.path : entry.stat(follow_symlinks=False).st_size})
            total += entry.stat(follow_symlinks=False).st_size
    return total    

def recive_mes(s, filename_hash = None):
    global LOGS
    mes = None
    print("Reciving...")
    logs.debug("Reciving...") if LOGS else 0
    try:
        data = s.recv(3)
    except:
        pritnt(bc.red("Can not recieve"))
        logs.error("Can not recieve") if LOGS else 0
        return 666
    if int.from_bytes(data[:2], byteorder='little') != 666 and data != '':
        print(data)
        print(bc.red("Where is my Devil packet? Packet is empty"))
        logs.error("Packet is empty") if LOGS else 0
        return 666
    elif data == '':
        print(bc.red('Connection close from outside'))
        logs.error("Connection close from outside") if LOGS else 0
        return 666

    id_ = int.from_bytes(data[-1:], byteorder='little')
    # print('id = ' + str(id_))
    if id_ == 6:
        print("ERROR!")
        code = (int.from_bytes(s.recv(1), byteorder='little'))
        errors = {1 : 'Permission denied', 
            2 : 'Wrong password',
            3 : 'User not found',
            4 : 'Wrong data',
            5 : 'Old Token',
            6 : 'Not Enough Place',
            7 : 'File Currently Updating',
            8 : 'Already Active Account'}
        print(bc.red(str(code) + " -- " + errors[code]))
        logs.error(str(code) + " -- " + errors[code]) if LOGS else 0
        if code == 2 or code == 3 or code == 5 or code == 8:
            GL_exit()
        mes = 666 + code

    elif id_ == 3: #3  | filename[64] total[2] number[2] datasize[2] data
        lms = 70
        s.settimeout(10)
        data = s.recv(lms)
        box = b''
        while len(data) < lms:
            data += s.recv(lms - len(data))
        if filename_hash != struct.unpack('<64s',data[:64])[0].decode('utf-8'):
            print(struct.unpack('<64s',data[:64])[0])
            print("ID 3 *** Hash not right")
            logs.error("ID 3 *** Hash not right | " + struct.unpack('<64s',data[:64])[0]) if LOGS else 0
            return 666

        total_packets = int.from_bytes(data[-6:-4], byteorder='little')
        numb_packet = int.from_bytes(data[-4:-2], byteorder='little')
        lms = 73 
        try:  
            while numb_packet < total_packets:
                # print(bc.red("Total:" + str(total_packets)))
                # print(bc.red("Number:" + str(numb_packet)))
                datasize = int.from_bytes(data[-2:], byteorder='little')
                tmp = s.recv(datasize)
                while len(tmp) < datasize:
                    tmp += s.recv(datasize - len(tmp))

                box += tmp
                data = s.recv(lms)
                while len(data) < lms:
                    data += s.recv(lms - len(data))

                total_packets2 = int.from_bytes(data[-6:-4], byteorder='little')
                numb_packet2 = int.from_bytes(data[-4:-2], byteorder='little')
                if total_packets2 != total_packets or numb_packet2 - numb_packet != 1 :
                    print("ID 3 *** packets != resiving packets")
                    logs.error("ID 3 *** Hash not right | " + struct.unpack('<64s',data[:64])[0]) if LOGS else 0
                    return 666

                numb_packet = numb_packet2

            datasize = int.from_bytes(data[-2:], byteorder='little')
            #print(bc.red("Datasize:" + str(datasize)))
            tmp = s.recv(datasize)
            #print(bc.waring("Datasize:" + str(datasize)))
            while len(tmp) < datasize:
                tmp += s.recv(datasize - len(tmp))
            box += tmp
        except (ConnectionRefusedError, socket.timeout) as e:
            return 666
        # print(box)
        mes = box

    elif id_ == 8:
        lms = 128
        data = s.recv(lms)
        while len(data) < lms:
            data += s.recv(lms - len(data))
        mes = packets(8, data)


    elif id_ == 10 or id_ == 12 or id_ == 14 or id_ == 16:
        data = s.recv(2)
        box = b''

        try:
            total_packets = int.from_bytes(data[0], byteorder='little')
            numb_packet = int.from_bytes(data[1], byteorder='little')
        except:
            total_packets = data[0]
            numb_packet = data[1]

        while numb_packet < total_packets:
            datasize = int.from_bytes(s.recv(2), byteorder='little')
            tmp = s.recv(datasize)
            while len(tmp) < datasize:
                tmp += s.recv(datasize - len(tmp))
            box += tmp
            data = s.recv(7)
            try:
                total_packets = int.from_bytes(data[0], byteorder='little')
                numb_packet = int.from_bytes(data[1], byteorder='little')
            except:
                total_packets = data[0]
                numb_packet = data[1]

            # if total_packets2 != total_packets or numb_packet2 - numb_packet != 1 :
            #     print("ID " + str(id_) + " *** packets != resiving packets")
            #     sys.exit()

        datasize = int.from_bytes(s.recv(2), byteorder='little')
        tmp = s.recv(datasize)
        while len(tmp) < datasize:
            tmp += s.recv(datasize - len(tmp))
        box += tmp
        mes = box.decode('utf-8')

    elif id_ == 18:
        datasize = int.from_bytes(s.recv(2), byteorder='little') + 1 
        data = s.recv(datasize)
        while len(data) < datasize:
            data += s.recv(datasize - len(data))

        if filename_hash != data[:-1].decode('utf-8'):
            print("ID " + str(id_) + " *** wrong path from packet") 
            logs.error("ID " + str(id_) + " *** wrong path from packet") if LOGS else 0
            return 666

        mes = int.from_bytes(data[-1:], byteorder='little')

    elif id_ == 20:
        datasize = int.from_bytes(s.recv(2), byteorder='little')
        data = s.recv(datasize)
        while len(data) < datasize:
            data += s.recv(datasize - len(data))

        old_path = data.decode('utf-8')
        datasize = int.from_bytes(s.recv(2), byteorder='little')
        data = s.recv(datasize)
        while len(data) < datasize:
            data += s.recv(datasize - len(data))

        new_path = data.decode('utf-8')
        if not filename_hash in new_path:
            print("ID " + str(id_) + " *** wrong path from packet")
            logs.error("ID " + str(id_) + " *** wrong path from packet") if LOGS else 0
            return 666
        data = s.recv(1)
        try:
            mes = int.from_bytes(data, byteorder='little')
        except:
            mes = data

    elif id_ == 36:
        lms = 65
        data = s.recv(lms)
        while len(data) < lms:
            data += s.recv(lms - len(data))
        if filename_hash != struct.unpack('<64s', data[:-1])[0].decode('utf-8'):
            print("ID " + str(id_) + " *** wrong filename from packet") 
            logs.error("ID " + str(id_) + " *** wrong path from packet") if LOGS else 0
            return 666
        mes = int.from_bytes(data[-1:], byteorder='little')

    return mes

def auth_tree(log,pas):
    global NS, TOKEN, TREE, SYSTEM_INFO            
    ns_host = NS["IP"] 
    ns_port = NS["port"]
    s = socket.socket() 

    try:
        s.connect((ns_host, ns_port))
        data = [log, pas]
        mes = packets(7, data)
        s.send(mes)
        TOKEN = recive_mes(s)
        s.send(packets(9, TOKEN))
        TREE =  json.loads(recive_mes(s))
        # print(TREE)
        SYSTEM_INFO = TREE[0]
        TREE = TREE[1:]
        s.close()
    except (ConnectionRefusedError,  socket.timeout) as e:
        print(bc.red("Connection to NS is not available"))


    # print(TREE)

def download(path):
    #13 | token[128] size[2] filepath  |  client to ns - get file request                            
    global NS, ROOT, LOGS
    ns_host = NS["IP"] 
    ns_port = NS["port"]
    print(bc.waring("Start downloading: " + path))
    logs.info("Start downloading: " + path) if LOGS else 0
    s = socket.socket()
    try:                    
        s.connect((ns_host, ns_port))
    except (ConnectionRefusedError, socket.timeout) as e:
        print(bc.red("Can not connect to NS for downloading"))
        logs.error("Can not connect to NS for downloading") if LOGS else 0
        return False
    data = path
    mes = packets(13, data)
    s.send(mes)
    INFO_FILE  = recive_mes(s)
    if not INFO_FILE:
        print(bc.red("Recived nothing"))
        logs.error("Recived nothing") if LOGS else 0
        s.close()
        return False
    try:    
        if INFO_FILE >= 666 and INFO_FILE <= 675:
            s.close()
            return False
    except:
        pass

    s.close()
    # print("info:")
    # print(INFO_FILE)
    INFO_FILE = json.loads(INFO_FILE)
    sources = INFO_FILE['components']
    # print("sources:")
    # print(sources)
    if not sources:
        print(bc.waring("Downloading file does not exist on the NS databasa"))
        return
    # #---------------------------Processing Download
    sort_on = "file_order"
    try:
        sort_sources = [(dict_[sort_on], dict_) for dict_ in sources]
        sort_sources.sort()
        sources = [dict_ for (key, dict_) in sort_sources] 
    except:
        tmp = []
        order = 1
        max_order  = len(sources)
        for orde in range(order, len(sources) + 1):
            for item in sources:
                tmp.append(item) if item["file_order"] == orde else 0
        sources = tmp
    #----------------------------------------------- Max replica
    max_replica = 0
    for i in sources:
        if i["replica"] > max_replica:
            max_replica =  i["replica"]

    
    print("Max replica: " + str(max_replica))
    logs.info("Max replica: " + str(max_replica)) if LOGS else 0
    #----------------------------------------------------------
    replica_number = 0
    something_wrong = True
    while something_wrong:
        something_wrong = False
        replica_number += 1
        print("Current replica: " + str(replica_number))
        logs.info("Current replica: " + str(replica_number)) if LOGS else 0
        if max_replica < replica_number:
            print(bc.red("Replica is over. All ways are wrong"))
            logs.error("Replica is over. All ways are wrong") if LOGS else 0
            return False
        max_file_order = sources[len(sources)-1]['file_order']
        print("Maximum chuncks: " + str(max_file_order))
        logs.info("Maximum chuncks: " + str(max_file_order)) if LOGS else 0
        file_order = 1
        f = open(ROOT + '/' + path, 'wb')
        for chunck in sources:
            if chunck["replica"] == replica_number and chunck["file_order"] == file_order:
                st_host = chunck['ip']
                st_port = int(chunck['port'])
                # print(chunck)

                st = socket.socket()
                try: 
                    st.connect((st_host, st_port))
                    data = chunck["filename"]
                
                    # #2  | token[128] filename[128]  | client to storage - get file chunk request
                
                    mes = packets(2, data)
                    # print(mes)
                    st.send(mes)
                    #3  | filename[128] total[2] number[2] datasize[2] data  | storage to client - send file chunk
                    FILE_content = recive_mes(st, data)
                    try:
                        if FILE_content >= 666 or FILE_content <= 675:
                            st.close()
                            f.close()
                            f = open(ROOT + '/' + path, 'wb')
                            f.close()
                            return False
                    except:
                        pass
                    f.write(FILE_content)
                    file_order += 1
                    st.close()
                except (ConnectionRefusedError,  socket.timeout) as e:
                    print(bc.waring("Server " + st_host + ": " + str(st_port) + " is not available"))
                    f.close()
                    f = open(ROOT + '/' + path, 'wb')
                    f.close()
                    something_wrong = True                    
    print(bc.waring("\nDownloading complete"))
    logs.info("Downloading complete") if LOGS else 0
    f.close()
    ind = LIST.index(ROOT + '/'+ path)
    os.utime(ROOT + '/' + path, ( INFO_FILE['accessed'], TREE[ind]['modified']))
    return True

def ns_delete(path):

    global NS, TREE, LIST, ROOT, LOGS       
    ns_host = NS["IP"] 
    ns_port = NS["port"]
    print('Deleting ' + path + " (to Trash)")
    s = socket.socket()
    try:
        s.settimeout(10)                    
        s.connect((ns_host, ns_port))
        if os.path.isdir(ROOT + '/'+ path):
            path = path + '/'
        data = path
        mes = packets(17, data)
        s.send(mes)
        #18 | size[2] filepath T/F[1] | ns to client - file delete result
        answer = recive_mes(s, path)
        s.close()

    except (ConnectionRefusedError,  socket.timeout) as e:
        print(bc.red("Can not connect to NS for deleting "+ path))
        logs.error("Can not connect to NS for deleting "+ path) if LOGS else 0 
        return False

    if answer == 1:
        print(bc.waring("File removed from NS basa"))
        logs.info("File removed from NS basa") if LOGS else 0
    else: 
        print(bc.red("File didn't remove"))
        logs.error("File didn't remove") if LOGS else 0
        return False

    print(LIST)
    print(ROOT + '/'+ path)
    if os.path.isdir(ROOT + '/'+ path):
        tmp = TREE
        TREE = []
        LIST = []
        for item in tmp:
            if not (ROOT + "/" + path) in str(ROOT + "/" + item["filepath"]):
                TREE.append(item)
                LIST.append(ROOT + "/" + item["filepath"])
                
    else:
        ind = LIST.index(ROOT + '/'+ path)
        del TREE[ind]
        del LIST[ind]
    print(LIST)
    return True

def ns_updatefile(path, option_path = "", next_attempt = {}, id_cur_chunk = 0):
    #15 | token[128] size[2] filepath *total[2] *number[2] datasize[2] data | client to ns - update file request
    global NS, ROOT, TREE, LIST, LOGS           
    ns_host = NS["IP"] 
    ns_port = NS["port"]
    if os.path.isdir(ROOT + "/" + path) and path[-1] != '/':
        path = path + '/'
    if os.path.isfile(ROOT + "/" + path):
        info = generate_info(path)
    elif os.path.isdir(ROOT + "/" + path[:-1]):
        info = generate_info(path[:-1])
    elif option_path == "": 
        info = {"accessed": time.time() , "created": time.time(), 
            "modified": time.time(), "filesize": 0, "filepath": path}
    else:   
        info = generate_info(option_path)
    jinfo = json.dumps(info)
    # print(jinfo)
    if not next_attempt:
        try:
            s = socket.socket()                    
            s.connect((ns_host, ns_port))
            data = [path, jinfo]
            mes = packets(15, data)
            s.send(mes)
            INFO_DISTR  = recive_mes(s)
            # print(INFO_DISTR)
            try:
                if INFO_DISTR >= 666 and INFO_DISTR <= 675:
                    s.close()
                    return False
            except:
                pass
            s.close()

        except (ConnectionRefusedError,  socket.timeout) as e:
            print(bc.red("Can not connect to NS for updating "+ path))
            logs.error("Can not connect to NS for updating "+ path) if LOGS else 0
            return False 

        INFO_DISTR = json.loads(INFO_DISTR)
    else:
        INFO_DISTR = next_attempt

    if os.path.isdir(ROOT + "/" + path[:-1]):
        if ROOT + '/' + path in LIST:
            ind = LIST.index(ROOT + '/'+ path)
            TREE[ind] = info
        else:
            TREE.append(info)
            LIST.append(ROOT + '/' + path)
        print(bc.waring("Uploaded dir: " + path))
        logs.info("Uploaded dir: " + path) if LOGS else 0
        return 

    # print("info:")
    # print(INFO_DISTR)
    
    to_sources = INFO_DISTR['components']

    # #---------------------------Processing Download
    sort_on = "file_order"
    try:
        sort_sources = [(dict_[sort_on], dict_) for dict_ in to_sources]
        sort_sources.sort()
        to_sources = [dict_ for (key, dict_) in sort_sources] 
    except:
        tmp = []
        order = 1
        max_order  = len(to_sources)
        for orde in range(order, len(to_sources) + 1):
            for item in to_sources:
                tmp.append(item) if item["file_order"] == orde else 0
        to_sources = tmp


    # print(to_sources) # Debug, where to load
    os.chmod(ROOT + '/' + path, S_IREAD|S_IRGRP|S_IROTH)
    f = open(ROOT + '/' + path,'rb')
    first_time = True
    for it in range(id_cur_chunk,len(to_sources)):
        chunck =  to_sources[it]
        if first_time:
            if id_cur_chunk != 0:
                tmp_tmp = f.read(chunck["offset"])
            first_time = False

        st_host = chunck["ip"]
        st_port = int(chunck['port'])
        st = socket.socket()
        st.settimeout(10)
        data = chunck["filename"]

        try: 
            print("Connection to "+ st_host+":"+str(st_port))
            logs.info("Connection to "+ st_host+":"+str(st_port)) if LOGS else 0
            st.connect((st_host, st_port))  
            mes_orig  = packets(35, data)                 
            st.send(mes_orig) 
            mes_orig = recive_mes(st, data)
            if mes_orig == 1:
                print(bc.waring("Access loading on server"))
                logs.info("Access loading on server") if LOGS else 0
            else:
                print(bc.red("Denied loading on server"))
                logs.error("Denied loading on server") if LOGS else 0
                f.close()
                return False

            total_packets = int(chunck["filesize"] / 2048) + 1
            numb_packet = 1
            #print("total = "+ str(total_packets))
            while numb_packet <= total_packets:
                if numb_packet != total_packets:
                    data = [chunck["filename"], f.read(2048)]
                else:
                    data = [chunck["filename"], f.read(chunck["filesize"] % 2048)]
                # print(len(data[1]))
                mes = packets(21, data, total_packets, numb_packet)
                st.send(mes)
                numb_packet += 1

        except (ConnectionRefusedError,  socket.timeout) as e:
            s.close()
            print(bc.waring("Server " + st_host + ": " + str(st_port) + " is not available"))
            ok = False
            timer = time.time()
            while not ok:
                time.sleep(20)
                try: 
                    print("Connection to "+ st_host+":"+str(st_port))
                    st.connect((st_host, st_port))
                    ok = True
                    new_source =  to_sources
                    return ns_updatefile(path, next_attempt=new_source, id_cur_chunk= it)
                except:
                    pass

                if time.time() - timer > 20:#240.0 and not ok:

                    print(bc.waring("Time is over. Alert NS ..."))
                    ns_host = NS["IP"] 
                    ns_port = NS["port"]
                    s = socket.socket()  
                    s.settimeout(15)
                    s.connect((ns_host, ns_port))
                    succes_list = []
                    if it - id_cur_chunk > 1:
                        for i in range(id_cur_chunk,it):
                            succes_list.append(to_sources[i-1]["filename"])

                    succes_list = {"filepath": path, "data": succes_list, "node": { "ip": to_sources[it]["ip"], "port": to_sources[it]["port"]}} 
                    succes_list = json.dumps(succes_list)
                    total_packets = int(len(data) / 2048) + 1
                    numb_packet = 1
                    while numb_packet <= total_packets:
                        data = succes_list[(numb_packet-1)*2048:(numb_packet)*2048]
                        mes = packets(38, data, total_packets, numb_packet)
                        s.send(mes)
                        numb_packet += 1

                    NEW_INFO  = recive_mes(s)
                    try:
                        if NEW_INFO >= 666 and NEW_INFO <= 675:
                            return False
                    except:
                        pass
                    new_source = json.loads(NEW_INFO)
                    s.close()

                    if new_source["continue"] == 0:
                        print(bc.waring("NS said not continue. It will resend all chuncks"))
                        f.close()
                        os.chmod(ROOT + '/' + path, S_IWUSR|S_IREAD)    
                        return ns_updatefile(path, next_attempt=new_source)
                        
                    else:
                        print(bc.waring("NS said continue. It will continue from current chunck"))
                        f.close()
                        os.chmod(ROOT + '/' + path, S_IWUSR|S_IREAD)
                        return ns_updatefile(path, next_attempt=new_source, id_cur_chunk= it)

        st.close()
    f.close()

    # 16 | size[2] filepath T/F[1]                                         | ns to client - file update result

    os.chmod(ROOT + '/' + path, S_IWUSR|S_IREAD)
    if ROOT + '/' + path in LIST:
        ind = LIST.index(ROOT + '/'+ path)
        TREE[ind] = info
    else:
        TREE.append(info)
        LIST.append(ROOT + '/' + path)
    # print("Tree:")
    # print(TREE)
    # print(LIST)
    print(bc.waring("Uploaded: " + path))
    logs.info("Uploaded: " + path) if LOGS else 0
    return

def ns_rename(old_path, new_path):
    #19 | token[128] size[2] srcfilepath size[2] dstfilepath              | client to ns - rename file request
    global NS, ROOT, TREE, LIST           
    ns_host = NS["IP"] 
    ns_port = NS["port"]

    if os.path.isdir(ROOT+"/"+old_path):
        data = [old_path + "/", new_path + "/"]
    else:
        data = [old_path, new_path]
    mes = packets(19, data)
    s = socket.socket()
    try:                  
        s.connect((ns_host, ns_port))
        s.send(mes)
    #20 | size[2] srcfilepath size[2] dstfilepath T/F[1]                  | ns to client - rename file result
        mes = recive_mes(s, new_path)
        s.close()
    except (ConnectionRefusedError,  socket.timeout) as e:
        print(bc.red("Can not connect to NS for renaming from "+ old_path + " to "+ new_path))
        logs.error("Can not connect to NS for renaming from "+ old_path + " to "+ new_path)
        return False

    if mes == 1:
        print(bc.waring("Rename successfully on NS"))
        logs.info("Rename successfully on NS")
    else:
        print(bc.red("Rename fail on NS"))
        logs.error("Rename fail on NS")
        return False

    if os.path.isdir(ROOT + '/'+ old_path):
        for item in LIST:
            if ROOT + '/'+ old_path in item:
                ind = LIST.index(item)
                TREE[ind]['filepath'].replace(old_path, new_path)
                LIST[ind].replace(old_path, new_path)
    else:
        ind_o = LIST.index(ROOT + '/'+ old_path)
        TREE[ind_o]["filepath"] = new_path
        LIST[ind_o] = ROOT + '/'+ new_path

    print(bc.waring("Renamed: " + old_path + " to " + new_path))
    return True

class logs:
    logging.basicConfig(format = u'[%(asctime)s] %(message)s', level = logging.DEBUG, filename = u'logs.txt')
    def debug(inf):
        logging.debug(inf)
    def info(inf):
        logging.info(inf)
    def waring(inf):
        logging.waring(inf)
    def error(inf):
        logging.error(inf)
    def critical(inf):
        logging.critical(inf)



class Passthrough(Operations):
    global LOGS
    open_ = 0.0
    p = DEBUG
    def __init__(self, root):
        self.root = root

    # Helpers
    # =======

    def _full_path(self, partial):
        if partial.startswith("/"):
            partial = partial[1:]
        path = os.path.join(self.root, partial)
        return path

    # Filesystem methods
    # ==================


    def access(self, path, mode):
        full_path = self._full_path(path)
        if self.p:
            print("acces -- > " + full_path)
            logs.debug("acces -- > " + full_path) if LOGS else 0
        if not os.access(full_path, mode):
            raise FuseOSError(errno.EACCES)

    def chmod(self, path, mode):
        full_path = self._full_path(path)
        if self.p:
            print("chmod -" + str(mode) + "- > " + full_path )
            logs.debug("chmod -" + str(mode) + "- > " + full_path) if LOGS else 0
        return os.chmod(full_path, mode)

    def chown(self, path, uid, gid):
        full_path = self._full_path(path)
        if self.p:
            print("chown -- > " + full_path )
            logs.debug("chown -- > " + full_path) if LOGS else 0
        return os.chown(full_path, uid, gid)

    def getattr(self, path, fh=None):
        global LIST_FILES, ROOT, TOTAL_SIZE
        full_path = self._full_path(path)
        st = os.lstat(full_path)
        way, fi = path.rsplit('/', maxsplit=1)
        if self.p:
            print("getattr -- > " + full_path) 
            logs.debug("getattr -- > " + full_path) if LOGS else 0
        attr = dict((key, getattr(st, key)) for key in ('st_atime', 'st_ctime',
                     'st_gid', 'st_mode', 'st_mtime', 'st_nlink', 'st_size', 'st_uid'))

        if os.path.isdir(full_path) and time.time() - attr['st_atime'] > 0.025:
            if full_path[-1] == '/':
                open(full_path + ".qoK5L8xZWLAEpD6Hpuk4qoK5L8xZWLAEpD6Hpuk4.txt",'w').close()
                os.remove(full_path+ '.qoK5L8xZWLAEpD6Hpuk4qoK5L8xZWLAEpD6Hpuk4.txt')
            else:
                open(full_path + "/.qoK5L8xZWLAEpD6Hpuk4qoK5L8xZWLAEpD6Hpuk4.txt",'w').close()
                os.remove(full_path+ '/.qoK5L8xZWLAEpD6Hpuk4qoK5L8xZWLAEpD6Hpuk4.txt')
            print("Update atime -- > " + full_path) if self.p else 0
            logs.debug("Update atime -- > " + full_path) if LOGS else 0
        elif ".Trash-" not in full_path:
            TOTAL_SIZE =  get_tree_size(ROOT)
            try:
                attr['st_ctime'] = TREE[LIST.index(full_path)]['created']
                if LIST_FILES[full_path] == 0:
                    attr['st_size'] = TREE[LIST.index(full_path)]['filesize']
            except:
                pass

        return attr

    def readdir(self, path, fh):
        full_path = self._full_path(path)

        dirents = ['.', '..']
        if self.p:
            print("readdir -->" + full_path)
            logs.debug("readdir -- > " + full_path) if LOGS else 0
        if os.path.isdir(full_path):
            dirents.extend(os.listdir(full_path))
        for r in dirents:
            yield r

    def readlink(self, path):
        pathname = os.readlink(self._full_path(path))
        if self.p:
            print("readlink --> " + full_path)
            logs.debug("readlink -- > " + full_path) if LOGS else 0
        if pathname.startswith("/"):
            return os.path.relpath(pathname, self.root)
        else:
            return pathname

    def mknod(self, path, mode, dev):
        if self.p:
            print("mknod --> " + full_path)
            logs.debug("mknod -- > " + full_path) if LOGS else 0
        return os.mknod(self._full_path(path), mode, dev)

    def rmdir(self, path):
        if self.p:
            print("rmdir")
        full_path = self._full_path(path)
        return os.rmdir(full_path)

    def mkdir(self, path, mode):
        full_path = self._full_path(path)
        if self.p:
            print("mkdir --> " + full_path + " MOD: " + str(mode))
            logs.debug("mkdir -- > " + full_path + " MOD: " + str(mode)) if LOGS else 0
        mk = os.mkdir(full_path, mode)
        if ".Trash-" not in full_path:
            if not ns_updatefile(path[1:]):
                return False
        return mk

    def statfs(self, path):
        full_path = self._full_path(path)
        if self.p:
            print("statfs --> " + full_path)
            logs.debug("statfs -- > " + full_path) if LOGS else 0
        stv = os.statvfs(full_path)
        return dict((key, getattr(stv, key)) for key in ('f_bavail', 'f_bfree',
            'f_blocks', 'f_bsize', 'f_favail', 'f_ffree', 'f_files', 'f_flag',
            'f_frsize', 'f_namemax'))

    def unlink(self, path):
        full_path = self._full_path(path)
        if self.p:
            print("unlink --> " + full_path)
            logs.debug("unlink -- > " + full_path) if LOGS else 0
        if "/.Trash-" not in full_path:
            if full_path in LIST_FILES.keys():
                if not ns_delete(path):
                    return False

        return os.unlink(full_path)

    def symlink(self, name, target):
        full_path = self._full_path(target)
        if self.p:
            print("unlink --> " + full_path)
            logs.debug("unlink -- > " + full_path) if LOGS else 0
        return os.symlink(name, full_path)

    def rename(self, old, new):
        old_path = self._full_path(old)
        new_path =  self._full_path(new)
        if self.p:
            print("rename --> " + old_path + " to --> " + new_path)
            logs.debug("unlink -- > " + full_path) if LOGS else 0
        if ".goutputstream" in old or ".goutputstream" in new:
            return os.rename(old_path, new_path)
        if ".Trash-" in new and ".Trash-" not in old:
            print(bc.waring('Deleting ' + self._full_path(old)) + " (to Trash)")
            if not ns_delete(old[1:]):
                return False

        elif ".Trash-" in old and  ".Trash-" not in new:
            if not ns_updatefile(new[1:], option_path = old[1:]):
                return False
        elif ".Trash-" not in old and  ".Trash-" not in new:
            if not ns_rename(old[1:], new[1:]):
                return False

        return os.rename(old_path, new_path)

    def link(self, target, name):
        full_path_t = self._full_path(target)
        full_path_n = self._full_path(name)
        if self.p:
            print("unlink target --> " + full_path_t + "name --> " + full_path_n)
            logs.debug("unlink target --> " + full_path_t + "name --> " + full_path_n) if LOGS else 0
        return os.link(full_path_t, full_path_n)

    def utimens(self, path, times=None):
        full_path = self._full_path(path)
        if self.p:
            print("unlink --> " + full_path)
            logs.debug("unlink -- > " + full_path) if LOGS else 0
        return os.utime(full_path, times)

    # File methods
    # ============

    def open(self, path, flags):
        
        full_path = self._full_path(path)
        way, fi = full_path.rsplit('/', maxsplit=1)
        print('Open --> ' + fi)
        logs.info("Open --> " + full_path_t) if LOGS else 0
        if ".Trash-" not in full_path:
            if self.p:
                t = time.time() - os.lstat(way).st_atime
                print("Time befor open: " + str(t))
                logs.debug("Time befor open: " + str(t)) if LOGS else 0
            if time.time() - os.lstat(way).st_atime  > 0.025 and LIST_FILES[full_path] == 0:
                #print("Dowload --> "+full_path)
                if not download(path[1:]):
                    open(full_path,'w').close()
        return os.open(full_path, flags)


    def create(self, path, mode, fi=None):
        full_path = self._full_path(path)
        if self.p:
            print("create --> " + full_path)
            logs.debug("create -- > " + full_path) if LOGS else 0
        return os.open(full_path, os.O_WRONLY | os.O_CREAT, mode)

    def read(self, path, length, offset, fh):
        os.lseek(fh, offset, os.SEEK_SET)
        read_ = os.read(fh, length)
        if self.p:
            print("read --> " + path)
            logs.debug("read -- > " + path) if LOGS else 0
        #print(read_)
        return read_

    def write(self, path, buf, offset, fh):
        if self.p:
            print("write --> " + path)
            logs.debug("write -- > " + path) if LOGS else 0
        os.lseek(fh, offset, os.SEEK_SET)
        return os.write(fh, buf)

    def truncate(self, path, length, fh=None):
        full_path = self._full_path(path)
        if self.p:
            print("truncate --> " + path)
            logs.debug("truncate -- > " + path) if LOGS else 0
        with open(full_path, 'r+') as f:
            f.truncate(length)

    def flush(self, path, fh):
        if self.p:
            print("truncate --> " + str(fh))
            logs.debug("truncate -- > " + str(fh)) if LOGS else 0
        return os.fsync(fh)

    def release(self, path, fh):
        global TREE, LIST
        full_path = self._full_path(path)
        if self.p:
            print("truncate --> " + full_path)
            logs.debug("truncate -- > " + full_path) if LOGS else 0
        closer = os.close(fh)
        if ".Trash-" not in full_path:
            if full_path not in LIST:
                if not ns_updatefile(path[1:]):
                    return False
            else:
                ind = LIST.index(full_path)
                st = os.lstat(full_path)
                if TREE[ind]["modified"] != st.st_mtime:
                    if not ns_updatefile(path[1:]):
                        return False
        return closer

    def fsync(self, path, fdatasync, fh):
        if self.p:
            print("fsync --> " + str(fh))
            logs.debug("fsync -- > " + str(fh)) if LOGS else 0
        return self.flush(path, fh)


#def main(mountpoint, root):
def main():
    global TREE, ROOT, MOUNTPOINT, LIVE, LOGS, KEEP_ALIVE_GL, SYSTEM_INFO
    log = logs if LOGS else 0 # Activate logging

    root = ROOT
    mountpoint = MOUNTPOINT
    if not os.path.exists(mountpoint):
        os.makedirs(mountpoint)
    if not os.path.exists(root):
        os.makedirs(root)

    # ===================== LOGIN<
    ok = False
    while not ok:
        ok = True
        log = input("Login: ")
        if not len(log) > 1:
            ok = False
            print("Length must be bigger then 1 symbol") 
        for ch in log:
            if not ok:
                break 
            if not ((ord(ch) > 47 and ord(ch) < 58) or (ord(ch) > 64 and ord(ch) < 91) or (ord(ch) > 96 and ord(ch) < 123)):
                ok = False
                print("You can use letters and digits only")
                break
    ok = False
    while not ok:
        ok = True
        passw = getpass.getpass()
        if not len(passw) > 1:
            ok = False
            print("Length must be bigger then 1 symbol") 
        for ch in passw:
            if not ok:
                break 
            if not ((ord(ch) > 47 and ord(ch) < 58) or (ord(ch) > 64 and ord(ch) < 91) or (ord(ch) > 96 and ord(ch) < 123)):
                ok = False
                print("You can use letters and digits only")
                break

    auth_tree(log, passw)
    print ("Successfully")
    if not os.path.exists(ROOT + "/" + log):
        for entry in os.scandir(ROOT):
            if entry.is_dir(follow_symlinks=False):
                shutil.rmtree(entry.path)
            else:
                os.remove(entry.path)
        ROOT +=  "/" + log
        os.makedirs(ROOT)
        root = ROOT
    else:
        ROOT +=  "/" + log
        root = ROOT

    LIVE = True
    KEEP_ALIVE_GL = Thread(target = keep_alive)
    KEEP_ALIVE_GL.start()
    signal.signal(signal.SIGINT, lambda signal, frame: GL_exit())
    # ===================== >

    global LIST_FILES, TOTAL_SIZE
    TOTAL_SIZE = get_tree_size(root)
    global LIST
    if TREE:
        for item in TREE:
            LIST.append(ROOT + "/" + item["filepath"])
    if LIST_FILES:
        print("Some stuff exists in root. It will load on server")   

    refresh = []
    load = []

    print("===========================\n" + bc.cyan("Exist files in directory (Totla Size: " 
        + str(TOTAL_SIZE) + " Bytes):"))
    for i in LIST_FILES.keys():
        print(i)
    print(bc.cyan("Files from NS (" + str(int(SYSTEM_INFO["total"] - SYSTEM_INFO["free"])) +  " Bytes):"))
    for i in LIST:
        print(i)
    print("===========================")

    for item in LIST_FILES.keys():
        if item in LIST and item[-1] != '/':
            stat = os.lstat(item)
            if stat.st_mtime > TREE[LIST.index(item)]['modified']:
                load.append(item)
            else:
                refresh.append(item)
        else:
            load.append(item)

    for item in LIST:
        if item not in LIST_FILES.keys():
            PATH, FILE = item.rsplit('/', maxsplit=1)            
            if FILE!="":
                tmp = open(item, 'w')
                tmp.close()
            else:
                os.makedirs(item)
    if load:
        print("Primary loading existing files:")
        logs.info("Primary loading existing files:") if LOGS else 0
        for item in load:
            print("Uploading: " +  item.replace(ROOT + "/",""))
            logs.info("Uploading: " +  item.replace(ROOT + "/","")) if LOGS else 0
            thread = Thread(target = ns_updatefile, args = (item.replace(ROOT + "/",""), ))
            thread.start()
        

    if refresh:
        print("Primary refreshing existing files:")
        logs.info("Primary refreshing existing files:") if LOGS else 0
        for item in refresh:
            print(item.replace(ROOT + "/",""))
            logs.info(item.replace(ROOT + "/","")) if LOGS  else 0
            os.utime(item, (TREE[LIST.index(item)]['accessed'], TREE[LIST.index(item)]['modified']))

    TOTAL_SIZE = get_tree_size(root)

    print(bc.cyan("Current size/total size: " + str(TOTAL_SIZE) + '/' + str(SYSTEM_INFO["total"])))
    logs.waring("Current size/total size: " + str(TOTAL_SIZE) + '/' + str(SYSTEM_INFO["total"])) if LOGS else 0
    FUSE(Passthrough(root), mountpoint, nothreads=True, foreground=True, nonempty=True)

if __name__ == '__main__':
    signal.signal(signal.SIGINT, lambda signal, frame: exit())
    main()
