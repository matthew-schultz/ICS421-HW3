# !/bin/bash
test-cases/test1/test1-ms-1.pre
python3 runSQL.py test-cases/test1/test1-ms-1.cfg test-cases/test1/test1-ms-1.sql | sort >  test-cases/test1/test1-ms-1.out
test-cases/test1/test1-ms-1.post | sort > test-cases/test1/test1-ms-1.post.out
diff test-cases/test1/test1-ms-1.post.out test-cases/test1/test1-ms-1.post.exp
