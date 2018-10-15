# MockPySpark

See PySpark Documentation for RDD method usage [https://spark.apache.org/docs/latest/api/python/index.html](https://spark.apache.org/docs/latest/api/python/index.html)

#### Debugging Your Pipeline

Use the bellow method to initialise a mock spark context. To show debug add the debug true flag. To print data as json rather that dataframes add the json true flag. If your pipeline contains a textfile method you must define a real spark contect to handle the reading of the file.
```python
from sparkcontext import MockSparkContext
sc = MockSparkContext(debug=True, json=False, realsparkcontext=None)
```  

Define some spark methods to be used in your data pipeline. Any docs added to the method will be visible in the debug output.
```python
def applyPercentage(row):
    '''
    :param (subject, (percentage, mark)):
    :return: (subject, percentage*mark/100)
    '''
    return (row[0], row[1][0] * row[1][1] / 100.0)

```

```python
def sumResults(a,b):
    '''
    :param a: result 1
    :param b: result 2
    :return: result 1 + result 2
    '''
    return a + b
```

##### Write Your Spark Pipeline

```python
rdd1 = sc.parallelize([
             ("math",    55),
             ("math",    56),
             ("english", 57),
             ("english", 58),
             ("science", 59),
             ("science", 54)])

rdd2 = sc.parallelize([
             ("math",    50),
             ("english", 25),
             ("science", 25)])

rdd4 = rdd1.union(rdd2)

results =  rdd3\
           .join(rdd4)\
           .map(applyPercentage)\
           .reduceByKey(sumResults)\
           .collect()
```

##### Terminal Output 

```commandline
            .------------.                                                   
            | parallelize |                                                  
            |             |                                                  
            '------------'                                                   
                    |                                                          TOTAL ROWS 3
                    |                                                          key  value
                    |                                                             math     55
                    |                                                             math     56
                    |                                                          english     57
                    |                                                          
                    |          .------------.                                
                    |          | parallelize |                               
                    |          |             |                               
                    |          '------------'                                
                    |                  |                                       TOTAL ROWS 3
                    |                  |                                       key  value
                    |                  |                                       english     58
                    |                  |                                       science     59
                    |                  |                                       science     54
                    |                  |                                       
                    |                  |          .------------.             
                    |                  |          | parallelize |            
                    |                  |          |             |            
                    |                  |          '------------'             
                    |                  |                  |                    TOTAL ROWS 3
                    |                  |                  |                    key  value
                    |                  |                  |                       math     50
                    |                  |                  |                    english     25
                    |                  |                  |                    science     25
                    |                  |                  |                    
               .------.                |                  |                  
               | union |---------------                   |                  
               |       |                                  |                  
               '------'                                   |                  
                    |                                     |                    TOTAL ROWS 6
                    |                                     |                    key  value
                    |                                     |                       math     55
                    |                                     |                       math     56
                    |                                     |                    english     57
                    |                                     |                    english     58
                    |                                     |                    science     59
                    |                                     |                    science     54
                    |                                     |                    
                .-----.                                   |                  
               | join |-----------------------------------                   
               |      |                                                      
                '-----'                                                      
                    |                                                          TOTAL ROWS 6
                    |                                                          key     value
                    |                                                          english  (25, 57)
                    |                                                          english  (25, 58)
                    |                                                             math  (50, 55)
                    |                                                             math  (50, 56)
                    |                                                          science  (25, 59)
                    |                                                          science  (25, 54)
                    |                                                          
        .---------------------.                                              
       | map(applyPercentage) |                                              
       |                      |                                              
        '---------------------'                                              
                    |                                                          TOTAL ROWS 6
                    |                                                          key  value
                    |                                                          english  14.25
                    |                                                          english  14.50
                    |                                                             math  27.50
                    |                                                             math  28.00
                    |                                                          science  14.75
                    |                                                          science  13.50
                    |                                                          
                    |                                                          DOCS
                    |                                                          :param (subject, (percentage, mark)):
                    |                                                          :return: (subject, percentage*mark/100)
                    |                                                          
      .------------------------.                                             
      | reduceByKey(sumResults) |                                            
      |                         |                                            
      '------------------------'                                             
                    |                                                          TOTAL ROWS 3
                    |                                                          key  value
                    |                                                          science  28.25
                    |                                                             math  55.50
                    |                                                          english  28.75
                    |                                                          
                    |                                                          DOCS
                    |                                                          :param a: result 1
                    |                                                          :param b: result 2
                    |                                                          :return: result 1 + result 2
                    |                                                          


```

### Unit Tests

Define the output of each stage of your data pipeline. Use the method name as a key and a json format for the expected contents of the pipeline after that method has run.

```python
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
```

##### Terminal Output

```commandline
Traceback (most recent call last):
    ...
    raise AssertionError(''.join(errormsg))
AssertionError: 
ERROR IN map(applyPercentage), "GOT"<-"EXPECTED"
key                 value
english                 14.25
english  14.5<-CORRECT_ANSWER
   math                  27.5
   math                    28
science                  13.5
science                 14.75
```
