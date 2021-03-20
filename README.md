# Database_Relation_Join_Algorithms
Join algorithms have been designed given M memory blocks and two large relations R(X,Y) and S(Y,Z).

## SortMerge Join
#### Execution
> python3 sortMerge.py <input_file1> <input_file2> <memory_buffers> 
#### Summary
- Create sorted sublists for R and S, each of size M blocks.
- Inbuilt sort function is used.
- Get one block from each sublists into memory and get minimum of R and S. Join this minimum Y value with the other table and store in the output buffer.
- When the output buffer is full write the block into the disk.
- When the block of a sublist brought into the memory is exhausted get the next block of the corresponding sublist. 
#### Constraints
- Number of blocks in relation R together with Number of blocks in relation S should not exceed square of the available main memory buffers.    
    
      B(R) + B(S) < M^2

## Hash Join
#### Execution
> python3 hashJoin.py <input_file1> <input_file2> <memory_buffers>
#### Summary
- Create M1 hashed sublists for R and S.
- Inbuilt hash function is used on the join attribute value.
- For each Ri and Si thus created, load the smaller of the two in the main memory and create a search structure over it.
- Recursively load the other file one block at a time and for each record of this file, search corresponding records (with same join attribute value) from the other file.
#### Constraints
- Either of the corresponding hashed sublists must be less than allocated memory.
  
        

