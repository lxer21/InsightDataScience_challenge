# -*- coding: utf-8 -*-
"""
Created on Wed Apr  5 18:57:03 2017

@author: lxer
"""

from time import ctime
import heapq
from operator import itemgetter
import datetime
import sys

def count_IPs_once(file_loc):
    IPs = {}
    with open(file_loc, 'r',encoding='latin-1') as f:
        for line in f:
            host=line.split(' ')[0]
            if host not in IPs:
                IPs[host] = 0
            IPs[host] += 1
    return(heapq.nlargest(10, IPs.items(), key=itemgetter(1)))

def count_Bandwidths_once(file_loc):
    ##Resources = {}                                       ##key:resource name value:frequency
    ##Bytes={}                                             ##key:resource name value:bytes_sent
    Bandwidths={}                                        ##key:resource name value:Bandwidth                                       
    with open(file_loc, 'r',encoding='latin-1') as f:
        for line in f:
            line=line.strip()
            website=line.split(' ')[6]
            bytes_sent=line.split(' ')[-1]
            if website is not '/':
                if website not in Bandwidths:
                    Bandwidths[website] = 0
                if (bytes_sent.isdigit()):
                    Bandwidths[website] += int(bytes_sent)          
    return(heapq.nlargest(10, Bandwidths.items(), key=itemgetter(1)))

def count_Hours_once(file_loc):                                      
    Hours={}                                                                             
    with open(file_loc, 'r',encoding='latin-1') as f:
        for line in f:
            line = line.strip()
            split_line=line.split(" ") 
            timestamp=split_line[3]+" "+split_line[4]
            if timestamp not in Hours:
                Hours[timestamp] = 0
            Hours[timestamp] += 1         
    return(heapq.nlargest(10, Hours.items(), key=itemgetter(1)))


def parse_line(line):                          ##str line
    split_line=line.split(" ")                 ##Take a single log line, and split it on the space character ( )
    #if len(split_line) < 9:
        #return []
    host=split_line[0]
    timestamp=split_line[3]+" "+split_line[4]
    request_type=split_line[5]
    request_source=split_line[6]
    ##client_protocol=split_line[7]
    ##request=split_line[5]+" "+split_line[6]+" "+split_line[7]
    HTTP_reply_code=split_line[-2]
    bytes_sent=split_line[-1]
    return [
        host,
        timestamp,
        request_type,
        request_source,
        HTTP_reply_code,
        bytes_sent,
    ]                                            ##return a list
    
def parse_time(time_str):
    try:
        time_obj = datetime.datetime.strptime(time_str, '[%d/%b/%Y:%H:%M:%S %z]')
    except Exception:
        time_obj = ""
    return time_obj
            
def attempt_window(f,IP,start_time,IPs):
    count_fail_login=1 
    end_time=start_time+datetime.timedelta(seconds = 20)
    while(True):
        temp=f.readline()
        split_line_1=parse_line(temp)                          ##explain temp
        IP_now=split_line_1[0]
        now=split_line_1[1]
        nowtime=parse_time(now) 
        HTTP_reply_code=split_line_1[4]
        if(nowtime>end_time):
            break
        if(IP_now==IP):
            IPs.append(f.tell())
            if(HTTP_reply_code=='200'):
                break
            elif(HTTP_reply_code=='401'):
                count_fail_login +=1
        if(count_fail_login==3):
            return(nowtime)                                ##go to 5_min_block_window
            break
    if(count_fail_login<3):
        return(-1)
                
                
def block_window(f,IP,start_time,IPs): 
    blocked=[]                                        
    end_time=start_time+datetime.timedelta(minutes=5)
    while(True):
        temp=f.readline()
        split_line_1=parse_line(temp)  
        IP_now=split_line_1[0]
        now=split_line_1[1]
        nowtime=parse_time(now) 
        if(nowtime>=end_time):
            break
        if(IP_now==IP):
            IPs.append(f.tell())
            blocked.append(temp)  
    return(blocked)   
 
def find_block(file_loc):
    f=open(file_loc,'r',encoding='latin-1')
    Blocked=[]
    IPs=[]                                                       ## creat a slack to save the position of line in the windows
    while(True):
        line=f.readline()
        if (len(line)==0):
            break
        split_line=parse_line(line)
        HTTP_reply_code=split_line[4]
        if ((HTTP_reply_code=='401')and(f.tell() not in IPs)):
            start_point=f.tell()                                    ##the beginning of next line,save the position
            IP=split_line[0]
            timestamp=split_line[1]
            start_time=parse_time(timestamp)                             ##datetime type                                         
            end_time=attempt_window(f,IP,start_time,IPs)   ##enter the 20_seconds_window
            if(end_time!=-1):
               blocked=block_window(f,IP,end_time,IPs)       ##enter the 5_minutes_window
               Blocked=Blocked+blocked
            f.seek(start_point)                                   ##return to the original point
        if (f.tell() in IPs): 
            IPs.remove(f.tell())
    f.close()
    return(Blocked)

if __name__ == '__main__':
    print(ctime())
    inFile = sys.argv[1]
    outFile_1 = sys.argv[2]
    outFile_2 = sys.argv[3]
    outFile_3 = sys.argv[4]
    outFile_4 = sys.argv[5]
    outputlist=count_IPs_once(inFile)  
    f1=open(outFile_1,'w')
    for item in outputlist:
        f1.write(str(item[0]+','+str(item[1])))
        f1.write('\n')
    f1.close()
    outputlist=count_Bandwidths_once(inFile)  
    f2=open(outFile_3,'w')
    for item in outputlist:
        f2.write(item[0])
        f2.write('\n')
    f2.close()
    outputlist=count_Hours_once(inFile)  
    f3=open(outFile_2,'w')
    for item in outputlist:
        f3.write(str(item[0].strip('[]')+','+str(item[1])))
        f3.write('\n')
    f3.close()   
    f4=open(outFile_4,'w')
    outputlist=find_block(inFile)   
    for item in outputlist:
        f4.write(item)
    f4.close()
    print(ctime())
