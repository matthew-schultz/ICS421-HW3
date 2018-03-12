UPDATE dtables
SET column1 = value1, column2 = value2, ...
WHERE nodeid=1;


SELECT EXISTS(SELECT 1 FROM dtables WHERE nodeid=5);
