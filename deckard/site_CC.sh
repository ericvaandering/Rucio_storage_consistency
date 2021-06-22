#!/bin/sh

i=0; 
for i in {1..5}
do 
echo "$i: $(date)"; 
i=$((i+1)); 
sleep 5; 
done
