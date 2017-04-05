ntuple_rdd.filter(lambda record: record.Sex != '"Both"') \
    .map(lambda record: ((record.Location,record.Time,record.Sex),float(record.Value))) \
    .reduceByKey(add) \
    .map(lambda record: ((record[0][0],record[0][1]),(record[0][2],record[1]))) \
    .combineByKey((lambda x: (x[0],x[1])),(lambda x,y: (x[0],x[1],y[0],y[1])),(lambda x,y: (x[0],x[1],y[0],y[1]))) \
    .take(2)
