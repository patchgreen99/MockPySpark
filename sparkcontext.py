from rdd import RDD
import pandas as pd
import numpy as np
from termcolor import colored
import pprint


LENGTH = 70 # number of characters from the left to the debug statements on the right
WINDOWS = 4 # number of seperate rdd tasks to make visible + 1

class MockSparkContext:
    def __init__(self, realsparkcontext=None, debug=False, json=False):
        '''
        :param realsparkcontext: required when using the textfile method in yourt data pipeline
        :param debug: true if you want to print debug
        :param json: true if you want json formatted output rather than dataframes
        '''
        self.realsc = realsparkcontext
        self.pipeline = []
        self.maxtask = 0
        self.closedtasks = []
        self.veryclosedtasks = []
        self.deadtasks = []
        self.debug = debug
        self.json = json
        self.edges = {}
        self.values = {}
        self.window = 0

    def setJobGroup(self, groupId, description, interruptOnCancel=False):
        pass

    def broadcast(self, dictionary):
        return Broadcast(dictionary)

    def accumulator(self, initial, subclass=None):
        return Accumulator(initial, subclass)

    def stop(self):
        if self.realsc:
            self.realsc.stop()

    def parallelize(self, c, numSlices=None):
        nextstage = self.getnextstage()
        self.values[nextstage] = c
        self.maxtask += 1
        self.addmethod("parallelize", thisstage=nextstage, thistask=self.maxtask)
        return RDD(self, stage=nextstage, task=self.maxtask)

    def emptyRDD(self):
        nextstage = self.getnextstage()
        self.values[nextstage] = []
        self.maxtask += 1
        self.addmethod("emptyRDD", thisstage=nextstage, thistask=self.maxtask)
        return RDD(self, stage=nextstage, task=self.maxtask)

    def textFile(self, name, minPartitions=None, use_unicode=False):
        nextstage = self.getnextstage()
        if self.realsc:
            self.values[nextstage] = self.realsc.textFile(name, minPartitions=minPartitions,
                                                          use_unicode=use_unicode).collect()
        else:
            self.values[nextstage] = self.values[0]
        self.maxtask += 1
        self.addmethod("textFile", thisstage=nextstage, thistask=self.maxtask)
        return RDD(self, stage=nextstage, task=self.maxtask)

    def validate(self, expectedoutput):
        '''
        :param expectedoutput: a dict containing the expected output of your data pipeline
        :return:
        '''
        for stage in range(max(self.values.keys()) + 1):
            name = self.pipeline[stage]

            rddstage = self.values[stage]

            if type(rddstage) == list:
                act = self.__toDataFrame(rddstage)
                self.__assertEqual(act, expectedoutput, name)

    def __generateLabel(self):
        yield '.' + '-'.join([''] * (len(self.pipeline[-1]) + 2)) + '.'
        yield '| {} |'.format(self.pipeline[-1])
        yield '| ' + ' '.join([''] * len(self.pipeline[-1])) + '  |'
        yield "'" + '-'.join([''] * (len(self.pipeline[-1]) + 2)) + "'"

    def __toDataFrame(self, listofdicts):
        result = []
        for row in listofdicts:
            (key, value) = row
            if type(value) == dict:
                value['_key'] = key
                result.append(value)
            else:
                result.append({'key': key, 'value': value})

        return pd.DataFrame(result).replace(np.nan, '')

    def addmethod(self, sparkmethod, method=None, thisstage=None, thistask=None):
        if method:
            methodname = sparkmethod + '(' + method.__name__ + ')'

            # shorted method name
            if len(methodname) > 35:
                offset = len(methodname) - 35
                method.__name__ = method.__name__[:-(offset + 2)] + '..'

            self.pipeline.append(sparkmethod + '(' + method.__name__ + ')')
        else:
            self.pipeline.append(sparkmethod)

        if self.debug:
            label = self.__generateLabel()
            for idx, row in enumerate(label):
                continuebranch = idx == 0
                print colored(''.join(self.__makemessage(thistask, row, continuebranch=continuebranch)), 'cyan')

            if self.values.get(thisstage):
                totrows = len(self.values[thisstage])
                print ''.join(self.__makemessage(thistask, colored('TOTAL ROWS {}'.format(totrows), 'green'), left=True))

                df = self.__toDataFrame(self.values[thisstage])
                datamsg = self.__buildDataMessage(df)
                for count, msg in enumerate(datamsg.split('\n')):

                    # limit the dataframe shown to 20 rows
                    if count > 20:
                        break
                    print ''.join(self.__makemessage(thistask, str(msg), left=True))

                if method:
                    print ''.join(self.__makemessage(thistask, '', left=True))
                    print colored(''.join(self.__makemessage(thistask, 'DOCS', left=True)), 'magenta')
                    for msg in method.__doc__.strip().split('\n'):
                        print colored(''.join(self.__makemessage(thistask, msg, left=True)), 'magenta')

                print ''.join(self.__makemessage(thistask, '', left=True))
            else:
                print ''.join(self.__makemessage(thistask, colored('TOTAL ROWS {}'.format(0), 'green'), left=True))

    def getnextstage(self):
        if self.values:
            return max(self.values.keys()) + 1
        else:
            return 0

    def __makemessage(self, thistask, text, left=False, continuebranch=False):
        branchstr = '|                  '
        blankstr = ' '.join([''] * (len(branchstr) + 1))
        buffer = []
        usededges = set()
        chars = 0
        for task in range(self.window, self.window + WINDOWS):
            if task == self.window:
                chars += 10
                buffer.append(' '.join([''] * 21))
                continue

            if task > self.maxtask:
                chars += len(blankstr)
                buffer.append(blankstr)
                continue

            if task == thistask:
                if not left and thistask in self.edges.keys() and task >= thistask and self.edges[
                    thistask] > task and not continuebranch:
                    msg = "{}".format(text)
                    buffer[-1] = buffer[-1][:-(1 + (len(msg) / 2))]
                    extra = '-'.join([''] * (len(branchstr) - (len(msg) - (1 + (len(msg) / 2))) + 1))
                    chars += len(msg + extra)
                    buffer.append(msg + extra)
                    usededges.add(thistask)

                elif not left:
                    msg = "{}".format(text)
                    buffer[-1] = buffer[-1][:-(1 + (len(msg) / 2))]
                    extra = ' '.join([''] * (len(branchstr) - (len(msg) - (1 + (len(msg) / 2))) + 1))
                    chars += len(msg + extra)
                    buffer.append(msg + extra)

                elif task not in self.deadtasks:
                    chars += len(branchstr)
                    buffer.append(branchstr)

                else:
                    self.window = max(self.deadtasks + self.veryclosedtasks)
                    chars += len(blankstr)
                    buffer.append(blankstr)

            elif thistask in self.edges.keys() and task > thistask - 1 and self.edges[
                thistask] > task and not continuebranch:
                chars += len(branchstr) + 1
                buffer.append('-'.join([''] * (len(branchstr) + 1)))
                usededges.add(thistask)

            elif task < thistask or task > thistask:
                if task not in self.closedtasks:
                    chars += len(branchstr)
                    buffer.append(branchstr)

                elif (continuebranch and task not in self.veryclosedtasks):
                    chars += len("|" + ' '.join([''] * (len(branchstr))))
                    buffer.append("|" + ' '.join([''] * (len(branchstr))))

                else:
                    chars += len(blankstr)
                    buffer.append(blankstr)

        if left:
            buffer.append(' '.join([''] * (LENGTH - chars)))
            buffer.append(text)

        for usededge in usededges:
            self.edges.pop(usededge)
        return buffer

    def __buildDataMessage(self, df):
        msg = ""
        pd.set_option('display.width', 1000)
        pd.set_option('display.max_columns', 1000)
        if not self.json:
            msg += df.to_string(index=False)
        else:
            msg += pprint.pformat(df.to_dict('records'))
        return msg

    def __assertEqual(self, act, expectedoutput, name):
        if expectedoutput.get(name):
            exp = pd.DataFrame(expectedoutput[name]).replace(np.nan, '')
            # drop non dupes columns
            for column in act.columns:
                if column not in exp.columns:
                    act = act.drop([column], axis=1)

            # collect columns to sort by
            forsort = []
            for idx, cols in enumerate(list(act.columns)):
                if type(act.iloc[-1, idx]) not in [list, set]:
                    forsort.append(cols)

            # sort data frames
            act = act.sort_values(sorted(forsort))
            exp = exp.sort_values(sorted(forsort))
            act = act.reset_index(drop=True)
            exp = exp.reset_index(drop=True)
            for column in exp:
                errormsg = []
                try:
                    # compare dataframes
                    if not exp[~(exp == act)][column].isnull().all():
                        mixedcolumns = act[~(exp == act)][column]\
                            .astype(str)\
                            .str\
                            .cat(exp[~(exp == act)][column]
                            .astype(str), sep='<-')

                        act.loc[~(exp[column] == act[column]), column] = mixedcolumns
                        errormsg = ['\n',
                                    colored('ERROR IN {},'.format(name) + ' "GOT"' + '<-' + '"EXPECTED"', 'red') + '\n',
                                    self.__buildDataMessage(act)]

                except:
                    errormsg = ['\n', colored('ERROR IN {},'.format(name), 'red') + '\n',
                                colored('"EXPECTED"', 'red') + '\n',
                                self.__buildDataMessage(act),
                                colored('"GOT"', 'red') + '\n',
                                self.__buildDataMessage(exp)]

                if errormsg:
                    raise AssertionError(''.join(errormsg))


class Accumulator:
    def __init__(self, initial, subclass=None):
        self.subclass = subclass
        if self.subclass:
            self.value = subclass.zero(initial)
        else:
            self.value = initial

    def add(self, initial):
        if self.subclass:
            self.value = self.subclass.addInPlace(self.value, initial)
        else:
            self.value += initial


class Broadcast:
    def __init__(self, initial):
        self.value = initial