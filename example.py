from sparkcontext import MockSparkContext

sc = MockSparkContext(debug=True, json=False)

def applyPercentage(row):
    '''
:param (subject, (percentage, mark)):
:return: (subject, percentage*mark/100)
    '''
    return (row[0], row[1][0] * row[1][1] / 100.0)

def sumResults(a,b):
    '''
:param a: result 1
:param b: result 2
:return: result 1 + result 2
    '''
    return a + b

rdd1 = sc.parallelize([
             ("math",    55),
             ("math",    56),
             ("english", 57)])
rdd2 = sc.parallelize([
             ("english", 58),
             ("science", 59),
             ("science", 54)])
rdd3 = sc.parallelize([
             ("math",    50),
             ("english", 25),
             ("science", 25)])

rdd4 = rdd1.union(rdd2)

results =   rdd3\
            .join(rdd4)\
            .map(applyPercentage)\
            .reduceByKey(sumResults)\
            .collect()

sc.validate({'map(applyPercentage)': [{'key': 'english', 'value': 14.25},
                                      {'key': 'english', 'value': 'CORRECT_ANSWER'},
                                      {'key': 'math', 'value': 27.5},
                                      {'key': 'math', 'value': 28.0},
                                      {'key': 'science', 'value': 14.75},
                                      {'key': 'science', 'value': 13.5}],

             'reduceByKey(sumResults)': [{'key': 'science', 'value': 28.25},
                                         {'key': 'math', 'value': 55.5},
                                         {'key': 'english', 'value': 'CORRECT_ANSWER'}]
             })

