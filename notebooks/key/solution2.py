ntuple_rdd.filter(lambda record: record.Sex != '"Both"') \
    .map(lambda record: ((record.Location,record.Time,record.Sex),float(record.Value))) \
    .reduceByKey(add) \
    .map(lambda record: ((record[0][0],record[0][1]),(record[0][2],record[1]))) \
    .groupByKey().mapValues(lambda xs: list(xs)) \
    .map(lambda record: (record[1][0][1]/record[1][1][1],(record[0][0],record[0][1]))) \
    .take(2)
