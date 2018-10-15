import copy


class DataFrameWriter:
    def partitionBy(self, *values):
        return self

    def format(self, record):
        return self

    def options(self, **options):
        return self

    def save(self, **options):
        pass


class RDD:
    def __init__(self, sc, stage=0, task=0):
        self.sc = sc
        self.stage = stage
        self.task = task
        self.sc.maxtask = max(self.sc.maxtask, task)
        self.pipeline = sc.pipeline
        self.values = sc.values
        self.write = DataFrameWriter()

    def __rowparse(self, row):
        # make sure rdd is a copy and not referencing
        if type(row) != tuple:
            return (copy.deepcopy(row), None)
        else:
            return copy.deepcopy(row)

    # SPARK METHODS #

    def isEmpty(self):
        return len(self.values[self.stage]) == 0

    def take(self, numvalues):
        return self.values[self.stage][:numvalues]

    def collect(self):
        return self.values[self.stage]

    def count(self):
        return len(self.values[self.stage])

    def mapPartitions(self, method):
        nextstage = self.sc.getnextstage()
        self.values[nextstage] = []
        for outtamap in method([self.__rowparse(row) for row in self.values[self.stage]]):
            self.values[nextstage].append(outtamap)
        self.sc.addmethod('mapPartitions', method, nextstage, self.task)
        return RDD(self.sc, stage=nextstage, task=self.task)

    def foreachPartition(self, method):
        nextstage = self.sc.getnextstage()
        method([self.__rowparse(row) for row in self.values[self.stage]])
        self.sc.closedtasks.append(self.task)
        self.sc.veryclosedtasks.append(self.task)
        self.sc.deadtasks.append(self.task)
        self.sc.addmethod('foreachPartition', method, nextstage, self.task)

    def map(self, method):
        nextstage = self.sc.getnextstage()
        self.values[nextstage] = []
        for row in self.values[self.stage]:
            row = self.__rowparse(row)
            self.values[nextstage].append(method(row))
        self.sc.addmethod('map', method, nextstage, self.task)
        return RDD(self.sc, stage=nextstage, task=self.task)

    def mapValues(self, method):
        nextstage = self.sc.getnextstage()
        self.values[nextstage] = []
        for row in self.values[self.stage]:
            (key, value) = self.__rowparse(row)
            self.values[nextstage].append((key, method(value)))
        self.sc.addmethod('mapValues', method, nextstage, self.task)
        return RDD(self.sc, stage=nextstage, task=self.task)

    def flatMap(self, method):
        nextstage = self.sc.getnextstage()
        self.values[nextstage] = []
        for row in self.values[self.stage]:
            row = self.__rowparse(row)
            for subrow in method(row):
                self.values[nextstage].append(subrow)
        self.sc.addmethod('flatMap', method, nextstage, self.task)
        return RDD(self.sc, stage=nextstage, task=self.task)

    def flatMapValues(self, method):
        nextstage = self.sc.getnextstage()
        self.values[nextstage] = []
        for row in self.values[self.stage]:
            (key, value) = self.__rowparse(row)
            for subvalue in method(value):
                self.values[nextstage].append((key, subvalue))
        self.sc.addmethod('flatMapValues', method, nextstage, self.task)
        return RDD(self.sc, stage=nextstage, task=self.task)

    def groupByKey(self):
        nextstage = self.sc.getnextstage()
        self.values[nextstage] = []
        lastkey = None
        collection = []
        todo = sorted(self.values[self.stage], key=lambda r: str(r[0]))
        while len(todo) > 0:
            (key, value) = self.__rowparse(todo.pop())
            if lastkey and lastkey != key:
                self.values[nextstage].append((lastkey, collection))
                collection = [value]
            else:
                collection.append(value)
            lastkey = key

        if lastkey is not None:
            self.values[nextstage].append((lastkey, collection))
        self.sc.addmethod("groupByKey", thisstage=nextstage, thistask=self.task)
        return RDD(self.sc, stage=nextstage, task=self.task)

    def reduce(self, method):
        nextstage = self.sc.getnextstage()
        lastvalue = None
        for todo in self.values[self.stage]:
            if lastvalue is not None:
                lastvalue = method(self.__rowparse(todo), lastvalue)
            else:
                lastvalue = self.__rowparse(todo)

        self.values[nextstage] = lastvalue
        self.sc.closedtasks.append(self.task)
        self.sc.veryclosedtasks.append(self.task)
        self.sc.deadtasks.append(self.task)
        self.sc.addmethod('reduce', method, nextstage, self.task)
        return self.values[nextstage]

    def reduceByKey(self, method):
        nextstage = self.sc.getnextstage()
        self.values[nextstage] = []
        lastkey = None
        lastvalue = None
        todo = sorted(self.values[self.stage], key=lambda r: str(r[0]))
        while len(todo) > 0:
            (key, value) = self.__rowparse(todo.pop())
            if lastvalue and lastkey == key:
                todo.append((key, method(value, lastvalue)))
                todo = sorted(todo, key=lambda r: str(r[0]))
                lastvalue = None
            elif lastvalue and lastkey != key:
                self.values[nextstage].append((lastkey, lastvalue))
                lastvalue = value
            else:
                lastvalue = value

            lastkey = key

        if lastkey is not None:
            self.values[nextstage].append((lastkey, lastvalue))
        self.sc.addmethod('reduceByKey', method, nextstage, self.task)
        return RDD(self.sc, stage=nextstage, task=self.task)

    def zipWithUniqueId(self):
        nextstage = self.sc.getnextstage()
        self.values[nextstage] = []
        for i, row in enumerate(self.values[self.stage]):
            (key, value) = self.__rowparse(row)
            self.values[nextstage].append(((key, value.copy()), i))
        self.sc.addmethod('zipWithUniqueId', thisstage=nextstage, thistask=self.task)
        return RDD(self.sc, stage=nextstage, task=self.task)

    def filter(self, method):
        nextstage = self.sc.getnextstage()
        self.values[nextstage] = []
        for i, row in enumerate(self.values[self.stage]):
            (key, value) = self.__rowparse(row)
            if method((key, value)):
                self.values[nextstage].append((key, value))
        self.sc.addmethod('filter', method, nextstage, self.task)
        return RDD(self.sc, stage=nextstage, task=self.task)

    def union(self, rdd2):
        nextstage = self.sc.getnextstage()
        rdd1values = self.values[self.stage]
        rdd2values = rdd2.values[rdd2.stage]
        self.values[nextstage] = rdd1values + rdd2values
        self.sc.closedtasks.append(max(self.task, rdd2.task))
        self.sc.edges[min(self.task, rdd2.task)] = max(self.task, rdd2.task)
        self.sc.addmethod('union', thisstage=nextstage, thistask=min(self.task, rdd2.task))
        self.sc.veryclosedtasks.append(max(self.task, rdd2.task))
        return RDD(self.sc, stage=nextstage, task=min(self.task, rdd2.task))

    def join(self, rdd2):
        nextstage = self.sc.getnextstage()
        rdd1values = sorted(self.values[self.stage], key=lambda r: str(r[0]))
        rdd2values = sorted(rdd2.values[rdd2.stage], key=lambda r: str(r[0]))
        self.values[nextstage] = []
        for i in range(len(rdd1values)):
            for j in range(len(rdd2values)):
                rdd1key, rdd1value = rdd1values[i]
                rdd2key, rdd2value = rdd2values[j]
                if rdd1key == rdd2key:
                    self.values[nextstage].append((rdd1key, (rdd1value, rdd2value)))

        self.sc.closedtasks.append(max(self.task, rdd2.task))
        self.sc.edges[min(self.task, rdd2.task)] = max(self.task, rdd2.task)
        self.sc.addmethod('join', thisstage=nextstage, thistask=min(self.task, rdd2.task))
        self.sc.veryclosedtasks.append(max(self.task, rdd2.task))
        return RDD(self.sc, stage=nextstage, task=min(self.task, rdd2.task))

    def leftOuterJoin(self, rdd2):
        nextstage = self.sc.getnextstage()
        rdd1values = sorted(self.values[self.stage], key=lambda r: str(r[0]))
        rdd2values = sorted(rdd2.values[rdd2.stage], key=lambda r: str(r[0]))
        self.values[nextstage] = []
        i, j = 0, 0
        while i < len(rdd1values):
            rdd1key, rdd1value = rdd1values[i]
            rdd2key, rdd2value = rdd2values[j]
            if rdd1key == rdd2key:
                self.values[nextstage].append((rdd1key, (rdd1value, rdd2value)))
                if j != len(rdd2values) - 1:
                    j += 1
                else:
                    i += 1
            elif rdd1key < rdd2key or j == len(rdd2values) - 1:
                i += 1
                self.values[nextstage].append((rdd1key, (rdd1value, None)))
            elif rdd1key > rdd2key:
                j += 1

        self.sc.closedtasks.append(max(self.task, rdd2.task))
        self.sc.edges[min(self.task, rdd2.task)] = max(self.task, rdd2.task)
        self.sc.addmethod('join', thisstage=nextstage, thistask=min(self.task, rdd2.task))
        self.sc.veryclosedtasks.append(max(self.task, rdd2.task))
        return RDD(self.sc, stage=nextstage, task=min(self.task, rdd2.task))

    def getNumPartitions(self):
        return 1

    def repartition(self, v):
        return self

    def coalesce(self, partions):
        return self

    def partitionBy(self, *values):
        return self

    def toDF(self):
        return self

    def cache(self):
        return self

    def persist(self, memory):
        return self