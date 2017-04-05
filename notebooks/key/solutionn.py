ntuple_rdd.filter(lambda record: record.Sex == '"Both"' and int(record.AgeGrpStart) not in [0,5,10,15,20]) \
    .map(mapp_record) \
    .reduceByKey(add) \
    .map(lambda record: ((record[0][0],record[0][1]),(record[0][2],record[1]))) \
    .groupByKey() \
    .mapValues(lambda xs: list(xs)) \
    .map(cal_ratio) \
    .take(10)
