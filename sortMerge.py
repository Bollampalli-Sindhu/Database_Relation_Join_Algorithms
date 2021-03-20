import sys
import os,math
import operator
from itertools import islice
import heapq
from datetime import datetime

BLK_SIZE = 100

def error(msg):
    print(msg)
    exit(0)

def write_records(f,records):
    for record in records:
        line=" ".join(record)
        line=line+"\n"
        f.write(line)
    
def get_record(line):
    lst=line.split(' ')
    try:
        if lst[-1][-1] == '\n':
            lst[-1]=lst[-1][0:-1] 
    except:
        print(lst)
    return lst

def get_recordSize(filename):
    f = open(filename, "r") 
    line = f.readline()
    return len(line)

class Element(object):
    def __init__(self, record,index,sort_idx):
        self.record = record
        self.index = index
        self.sort_idx = sort_idx
    def __lt__(self, other):
        return (self.record[self.sort_idx]< other.record[self.sort_idx])

def get_name(R):
    return R.filename.split('/')[-1]

class Relation():
    def __init__(self,R):
        self.filename = R
        self.record_size = get_recordSize(R)
        
        filesize = os.stat(self.filename).st_size
        self.NUM_REC = math.ceil(filesize /self.record_size)
        # print(self.NUM_REC,self.record_size,self.filename)

    def read_records(self,size,n):
        num_lines=int(min(size,self.NUM_REC-n*size))
        self.sublist_size[n] = num_lines
        
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
    
    def sort_sublists(self,temp,idx):
        size = M*BLK_SIZE
        self.NUM_SUBLISTS = math.ceil(self.NUM_REC/size)
        self.sublist_size = [0]*self.NUM_SUBLISTS
        self.temp = temp
        self.sort_idx = idx
        for i in range(self.NUM_SUBLISTS):
            records = self.read_records(size,i)
            records.sort(key=operator.itemgetter(idx))
            intermediate_file=self.temp+str(i)
            
            f=open(intermediate_file,'w')
            write_records(f,records)
            f.close()

    # def get_record(self,line):
    #     lst=[]
    #     start=0
    #     for len_ in self.Col_len:
    #         lst.append(line[start:start+len_])
    #         start=start+len_+1
    #     return lst

class SortMerge_Join():
    def __init__(self,R,S):
        self.R = R
        self.S = S
        if math.ceil(R.NUM_REC/BLK_SIZE) + math.ceil(R.NUM_REC/BLK_SIZE) >= M*M:
            error("Number of blocks exceeded")
        self.buffer = []
        self.buffer_idx = 0
        self.output = []
    
    def open(self):
        self.R.sort_sublists('temp0_',1)
        self.S.sort_sublists('temp1_',0)
        
        self.R_blk_idx = [0]*self.R.NUM_SUBLISTS
        self.S_blk_idx = [0]*self.S.NUM_SUBLISTS
        
        self.R_blk_exhausted = [0]*self.R.NUM_SUBLISTS
        self.S_blk_exhausted = [0]*self.S.NUM_SUBLISTS

        self.out = open(get_name(self.R)+"_"+get_name(self.S)+"_join.txt",'w') 

        ## get 1st block of every sublist of relation R
        self.R_queue = []
        self.R_temp_files = [None]*self.R.NUM_SUBLISTS
        for i in range(self.R.NUM_SUBLISTS):
            self.R_temp_files[i] = open(self.R.temp+str(i),'r')
            self.read_block(self.R,i,self.R_blk_idx[i],self.R_queue,self.R_temp_files[i])
            self.R_blk_idx[i] += 1

        ## get 1st block of every sublist of relation S
        self.S_queue = []
        self.S_temp_files = [None]*self.S.NUM_SUBLISTS
        for i in range(self.S.NUM_SUBLISTS):
            self.S_temp_files[i] = open(self.S.temp+str(i),'r')
            self.read_block(self.S,i,self.S_blk_idx[i],self.S_queue,self.S_temp_files[i])
            self.S_blk_idx[i] += 1
        
        
    
    def getnext(self):
        ##replace 'while' with 'if' inorder to join one tuple at a time
        while(len(self.R_queue)>0):
            ele = heapq.heappop(self.R_queue)
            i = ele.index
            r = ele.record
            if len(self.buffer)==0 or self.buffer[0][self.S.sort_idx] < r[self.R.sort_idx]:
                self.tuples_with_common_y(self.S,r[self.R.sort_idx])
                    
            if len(self.buffer)==0 or self.buffer[0][self.S.sort_idx] > r[self.R.sort_idx]:
                self.R_blk_exhausted[i] +=1
            else:
                s = (self.buffer[self.buffer_idx]).copy()
                del s[self.S.sort_idx]
                
                self.output.append(r+s)
                if len(self.output) == BLK_SIZE:
                    write_records(self.out,self.output)
                    self.output = []
                
                self.buffer_idx +=1
                if self.buffer_idx == len(self.buffer):
                    self.buffer_idx = 0
                    self.R_blk_exhausted[i] +=1
                else:
                    ele = Element(r,i,R.sort_idx)
                    heapq.heappush(self.R_queue,ele)
            
            if(self.R_blk_exhausted[i] == BLK_SIZE):
                self.read_block(self.R,i,self.R_blk_idx[i],self.R_queue,self.R_temp_files[i])
                self.R_blk_idx[i] += 1
                self.R_blk_exhausted[i]=0
        
        write_records(self.out,self.output)
        self.output = []

    def close(self):
        for i in range(self.R.NUM_SUBLISTS):
            name = self.R.temp + str(i) 
            self.R_temp_files[i].close()
            os.remove(name)
        
        for i in range(self.S.NUM_SUBLISTS):
            name = self.S.temp + str(i) 
            self.S_temp_files[i].close()
            os.remove(name)

    def read_block(self,R,i,n,pq,f):
        num_rec = R.sublist_size[i]
        total_blocks = math.ceil(num_rec/BLK_SIZE)
        num_lines = int(min(BLK_SIZE, num_rec - n*BLK_SIZE))
        if n >= total_blocks:
            return None
        
        lines=islice(f, num_lines)
        for line in lines:
            ele = Element(get_record(line),i,R.sort_idx)
            heapq.heappush(pq,ele)

    def tuples_with_common_y(self,S,y):
        self.buffer = []
        self.buffer_idx = 0
        while(len(self.S_queue)>0):
            ele = heapq.heappop(self.S_queue)
            i = ele.index
            r = ele.record
            
            if (r[S.sort_idx]>y):
                ele = Element(r,i,S.sort_idx)
                heapq.heappush(self.S_queue,ele)
                break
            elif r[S.sort_idx]==y:
                self.buffer.append(r)
            
            self.S_blk_exhausted[i] +=1
            if(self.S_blk_exhausted[i] == BLK_SIZE):
                self.read_block(self.S,i,self.S_blk_idx[i],self.S_queue,self.S_temp_files[i])
                self.S_blk_idx[i] += 1
                self.S_blk_exhausted[i]=0

args = sys.argv
if len(args) < 4:
    error("Incorrect arguments")

R = Relation(args[1])
S = Relation(args[2])
M = int(args[3])

startTime = datetime.now()

obj = SortMerge_Join(R,S)
obj.open()
obj.getnext()
obj.close()

endTime = datetime.now()
print("Time Taken:",endTime - startTime)