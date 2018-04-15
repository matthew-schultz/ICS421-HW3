#!/bin/bash
# test3
# prepopulate catalog and nodes and run a select statement on each node; compare the output of the select statement with a .exp file to see if the program ran correctly
test-cases/test3/test3-ms-1.pre
python3 runSQL.py test-cases/test3/test3-ms-1.cfg test-cases/test3/test3-ms-1.sql | sort > test-cases/test3/test3-ms-1.out
test-cases/test3/test3-ms-1.post #| sort > test-cases/test3/test3-ms-1.post.out
DIFF=$(diff test-cases/test3/test3-ms-1.out test-cases/test3/test3-ms-1.out.exp)
if [ "$DIFF" != "" ] 
then
    echo "Test failed; output file did not match"
    echo $DIFF
else
    echo "Test ran successfully; output files matched"
    # rm test-cases/test3/test3-ms-1.post.out
fi
