import sys
import os,math
import operator
from itertools import islice
from datetime import datetime
import heapq
BLK_SIZE = 100

def error(msg):
    print(msg)
    exit(0)

def sublists(temp):
    res = []
    for i in range(M-1):
        f = open(temp+str(i),'w')
        res.append(f)
    return res
 
def Number_of_records(filename):
    f = open(filename, "r") 
    line = f.readline()
    filesize = os.stat(filename).st_size
    num_records = math.ceil(filesize /len(line))
    f.close()
    return len(line),num_records

def get_record(line):
    lst=line.split(' ')
    try:
        if lst[-1][-1] == '\n':
            lst[-1]=lst[-1][0:-1] 
    except:
        print(lst)
    return lst

def read_block(num_rec,n,f):
    total_blocks = math.ceil(num_rec/BLK_SIZE)
    num_lines = int(min(BLK_SIZE, num_rec - n*BLK_SIZE))
    records = []
    
    if n >= total_blocks:
        return None
    
    lines=islice(f, num_lines)
    for line in lines:
        records.append(get_record(line))

    return records

def get_name(R):
    return R.filename.split('/')[-1]

def getnext(R1,R2,n):
    f1 = R1.temp_files[n]
    f2 = R2.temp_files[n]
    
    list1 = []
    for line in f1:
        list1.append(get_record(line))
    
    list1.sort(key=operator.itemgetter(R1.hash_idx))
    
    num_blks1 = math.ceil(R1.sublist_size[n]/BLK_SIZE)
    num_blks = math.ceil(R2.sublist_size[n]/BLK_SIZE)
    for i in range(num_blks):
        list2 = read_block(R2.sublist_size[n],i,f2)
        
        # j = M-num_blks1-1
        # while(i<num_blks and j>0):
        #     i += 1
        #     j -= 1
        #     temp = read_block(R2.sublist_size[n],i,f2)
        #     if temp is not None:
        #         list2 += temp

        list2.sort(key=operator.itemgetter(R2.hash_idx))
        j=0
        
        for rec in list2:
            temp = rec.copy()
            del temp[R2.hash_idx]
            
            while(j<len(list1) and rec[R2.hash_idx]>list1[j][R1.hash_idx]):
                j += 1
            
            k = j
            while(k<len(list1) and rec[R2.hash_idx]==list1[k][R1.hash_idx]):
                temp1 = list1[k]+temp
                line=" ".join(temp1)
                line=line+"\n"
                output.write(line)
                k += 1

class Relation():
    def __init__(self,R,hash_idx):
        self.filename = R
        self.hash_idx = hash_idx
        self.record_size,self.NUM_REC = Number_of_records(R)
        # print(self.NUM_REC,self.record_size,self.filename)

    def hash_sublists(self,temp):
        self.temp_files = sublists(temp)
        self.sublist_size = [0]*(M-1)
        size = M*BLK_SIZE
        n = math.ceil(self.NUM_REC/size)
        self.temp = temp
        
        for i in range(n):
            records = self.read_records(size,i)
            self.hash_records(records)
        
        for i in range(M-1):
            self.temp_files[i].close()

    def read_records(self,size,n):
        num_lines=int(min(size,self.NUM_REC-n*size))
        
        try:
            f= open(self.filename, "r")
        except :
            error("Error: read_records() - error in opening file: "+sefl.filename)
        
        f.seek((n*size)*self.record_size, 0)   
        lines=islice(f, num_lines)
        
        records=[]
        for line in lines:
            records.append(get_record(line))
        f.close()
        
        return records

    def hash_records(self,records):
        for record in records:
            i = hash(record[self.hash_idx])%(M-1)
            line=" ".join(record)
            line=line+"\n"
            self.temp_files[i].write(line)

def open_():
    R.hash_sublists("temp0_")
    S.hash_sublists("temp1_")
    for i in range(M-1):
        _,n1 = Number_of_records(R.temp+str(i))
        _,n2 = Number_of_records(S.temp+str(i))
        if(min(n1,n2)>=M):
            close()
            error("Number of blocks exceeded. Cannot perform join operation")
        R.sublist_size[i] = n1
        R.temp_files[i] = open(R.temp+str(i),'r')
        
        S.sublist_size[i] = n2
        S.temp_files[i] = open(S.temp+str(i),'r')

    global output
    output = open(get_name(R)+"_"+get_name(S)+"_join.txt",'w')

def join():
    for i in range(M-1):
        if(R.sublist_size[i] <= S.sublist_size[i]):
            getnext(R,S,i)
        else:
            getnext(S,R,i)

def close():
    for i in range(M-1):
        name = R.temp + str(i) 
        R.temp_files[i].close()
        os.remove(name)
        
        name = S.temp + str(i) 
        S.temp_files[i].close()
        os.remove(name)
    

args = sys.argv
if len(args) < 4:
    error("Incorrect arguments")

R = Relation(args[1],hash_idx=1)
S = Relation(args[2],hash_idx=0)
M = int(args[3])
startTime = datetime.now()

open_()  
join()     
close()
output.close()
endTime = datetime.now()
print("Time Taken:",endTime - startTime)