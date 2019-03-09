#!/bin/bash
# Basic while loop
counter=1
while [ $counter -le $1 ]
do
echo $counter
python3 producer.py '10.60.2.240'&
((counter++))
done
echo All done