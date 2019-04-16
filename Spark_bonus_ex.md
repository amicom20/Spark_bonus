p14 Bonus Exercise

data sample
```
2014-03-15:10:49:30,
Sorrento F22L,
40e61459-5448-4dc9-bb89-42e73a4e19cf,
2,
31,
39,
enabled,
enabled,
enabled,
82,
31,
60,
39.4463417571,
-114.736213453
```

A. Upload the devicestatus.txt file to HDFS.
```
hdfs dfs -put $DEVDATA/devicestatus.txt /loudacre/
```

B. Determine which delimiter to use (hint: the character at position 19 is the first use of the delimiter).
C. Filter out any records which do not parse correctly (hint: eachrecord should have exactly 14 values).
```
rdd1 = sc.textFile("/loudacre/devicestatus.txt")
rdd2 = rdd1.map(lambda line: line.split(line[19:20])).filter(lambda values: len(values) == 14)
```

D. Extract the date (first field), model (second field), device ID (thirdfield), and latitude and longitude (13th and 14th fieldsrespectively).
E. The second field contains the device manufacturer and model name (such as Ronin S2). Split this field by spaces to separate the manufacturer from the model (for example, manufacturerRonin, model S2). Keep just the manufacturer name.
```
rdd3 = rdd2.map(lambda v: (v[0],v[1].split(' ')[0],v[2],v[12],v[13]))
```

F. Save the extracted data to comma-delimited text files in the /loudacre/devicestatus_etl directory on HDFS.
```
rdd3.map(lambda v: ','.join(v)).saveAsTextFile("/loudacre/devicestatus_etl")
```

G. Confirm that the data in the file(s) was saved correctly.
```
hdfs dfs -tail /loudacre/devicestatus_etl/part-00000

2014-03-15:10:49:30,Sorrento,86d93f67-0287-4e85-8472-076aa8b9fa42,37.4969347594,-122.174978527
2014-03-15:10:49:30,Titanic,609d7e61-faec-488d-b3f0-fb680a1100f6,33.7189909197,-112.127791734
2014-03-15:10:49:30,MeeToo,de6b7b6b-5c01-4f88-948c-5733716ab410,33.1941652647,-116.374666652
2014-03-15:10:49:30,Sorrento,7c06751c-f692-473e-9143-9280594a9740,45.1387902196,-117.739092779
2014-03-15:10:49:30,Sorrento,40e61459-5448-4dc9-bb89-42e73a4e19cf,39.4463417571,-114.736213453
2014-03-15:10:49:30,Titanic,d4f2958f-27a9-49ec-b176-eca99e486b57,36.524950844,-115.056582933
2014-03-15:10:49:30,Ronin,b13ece99-62ab-4c9f-a366-6a06bd5e877f,38.4282665514,-121.25933863
2014-03-15:10:49:30,Titanic,e683c864-2c49-46dd-8aa4-4f10364f3cd2,37.5589514366,-121.212136808
2014-03-15:10:49:30,Sorrento,32af1a0b-ca7f-4906-9772-9eb9435e7e4c,33.7778202246,-108.575470704
2014-03-15:10:49:30,Ronin,a48a5559-d916-481b-84a9-5dce6272cce1,38.2596913494,-122.295712621
2014-03-15:10:49:30,iFruit,d86fbaa6-b71b-435f-a0bf-5304a202a70b,34.2415255221,-118.23526739

```

All Codes
```
hdfs dfs -put $DEVDATA/devicestatus.txt /loudacre/
rdd1 = sc.textFile("/loudacre/devicestatus.txt")
rdd2 = rdd1.map(lambda line: line.split(line[19:20])).filter(lambda values: len(values) == 14)
rdd3 = rdd2.map(lambda v: (v[0],v[1].split(' ')[0],v[2],v[12],v[13]))
rdd3.map(lambda v: ','.join(v)).saveAsTextFile("/loudacre/devicestatus_etl")
```
