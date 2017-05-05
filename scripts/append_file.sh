#!/bin/bash

echo "Appending file $1 $2 times..."
for var in {1..$2}
do
    echo "... some random text ..." | cat >> $1
done
echo "Done."
