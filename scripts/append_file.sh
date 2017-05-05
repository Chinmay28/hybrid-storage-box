#!/bin/bash

echo "Appending file $1 $2 times..."
for ((i=1; i<=$2; i++)); do
    echo "... some random text ..." | cat >> $1
done
echo "Done."
