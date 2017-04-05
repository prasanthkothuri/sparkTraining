ntuple_rdd.filter(lambda record: record.AgeGrp =='"100+"' and record.Time == '"2015"' and record.Sex == '"Both"') \
    .map(lambda record: (record.Location,int(float(record.Value)*1000))) \
    .reduceByKey(lambda x,y: x+y) \
    .map(lambda (x,y):(y,x)).sortByKey(False) \
    .collect() \
