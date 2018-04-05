#!/bin/bash
test-cases/test2/test2-ms-1.pre
python3 runSQL.py test-cases/test2/test2-ms-1.cfg test-cases/test2/test2-ms-1.csv >  test-cases/test2/test2-ms-1.out
# test-cases/test1/test1-ms-1.post | sort > test-cases/test1/test1-ms-1.post.out
# DIFF=$(diff test-cases/test1/test1-ms-1.post.out test-cases/test1/test1-ms-1.post.exp)
test-cases/test2/test2-ms-1.post
if [ "$DIFF" != "" ] 
then
    echo "Test failed; output file did not match"
else
    echo "Test ran successfully; output files matched"
fi
