#!/usr/bin/env bash

cd $1

# num of files
files=`find . | wc -l`
# do not count dot
files=$(( files - 1 ))
echo "files $files"

# size
du -sh .

# extensions
echo ""
find -type f  | sed -e 's/.*\.//' | sort | uniq -c

# number of lines for csv
echo ""
find . -name "*.csv" | xargs wc -l
