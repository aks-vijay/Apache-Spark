SELECT *, 
  SPLIT(name, ' ')[0] AS forename, 
  SPLIT(name, ' ')[1] AS surname 
FROM f1_processed.drivers1


SELECT *, date_format(dob, "dd-MM-yyyy" ) FROM f1_processed.drivers1
SELECT *, date_add(dob, 1 ) FROM f1_processed.drivers1
