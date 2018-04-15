#!/bin/bash
test-cases/test3/test3-ms-1.pre
#python3 runSQL.py test-cases/test3/test3-ms-1.cfg test-cases/test3/test3-ms-1.sql > test-cases/test3/test3-ms-1.out
test-cases/test3/test3-ms-1.post | sort > test-cases/test3/test3-ms-1.post.out
DIFF=$(diff test-cases/test3/test3-ms-1.post.out test-cases/test3/test3-ms-1.post.exp)
if [ "$DIFF" != "" ] 
then
    echo "Test failed; output file did not match"
    echo $DIFF
else
    echo "Test ran successfully; output files matched"
    # rm test-cases/test3/test3-ms-1.post.out
fi
