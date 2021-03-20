#!/bin/bash

if [ $3 = "sort" ]; then
   python3 sortMerge.py "$1" "$2" "$4"
else
  python3 hashJoin.py "$1" "$2" "$4"
fi

# python3 MergeSortJoin.py "$1" "$2" "$3" "$4"