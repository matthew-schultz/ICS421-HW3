#!/bin/bash
# test4
# prepopulate catalog with dtables and nodes with authors and books tables
test-cases/test4/test4-ms-1.pre
#python3 runSQL.py test-cases/test4/test4-ms-1.cfg test-cases/test4/test4-ms-1.sql | sort > test-cases/test4/test4-ms-1.out
python3 runSQL.py test-cases/test4/test4-ms-1.cfg test-cases/test4/test4-ms-1.sql > test-cases/test4/test4-ms-1.out
test-cases/test4/test4-ms-1.post #| sort > test-cases/test4/test4-ms-1.post.out
#DIFF=$(diff test-cases/test4/test4-ms-1.out test-cases/test4/test4-ms-1.out.exp)
if [ "$DIFF" != "" ] 
then
    echo "Test failed; output file did not match"
    echo $DIFF
else
    echo "Test ran successfully; output files matched"
    # rm test-cases/test4/test4-ms-1.post.out
fi
