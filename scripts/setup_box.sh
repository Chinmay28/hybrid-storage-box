#!/bin/bash

START=$(date +%s)

cd /home/ubuntu/hybrid-storage-box/scripts/

echo "creating 25 files with size ranges 17M to 3.5G..."
for var in {1..25}
do
    random="$(shuf -i1-210 -n1)"
    seed_size=17
    size=$(($random * $seed_size))
    size+="M"
    
    bash mkfile /home/ubuntu/storage_box/file$var $size
done

echo "providing random access counts(1-257) to each file..."
for var in {1..25}
do
    random=$(shuf -i1-257 -n1)
    echo "appending file$var $random times..."
    for ((i=1; i<=random; i++)); do
        echo "append text" | cat >> /home/ubuntu/storage_box/file$var
    done
done

echo "All done!"

END=$(date +%s)
DIFF=$(( $END - $START ))
echo "Script took $DIFF seconds."
