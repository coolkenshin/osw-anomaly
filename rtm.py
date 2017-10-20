#imorts
import os
import sys
import json
import copy
import time
import datetime
import math
import urllib2
import logging,logging.handlers
import traceback
import socket
#import threading
# import concurrent.futures
from collections import deque
import signal
from argparse import ArgumentParser
#from Configfile_PlaceHolder import Configfile_PlaceHolder
#from DataReader import DataReader
#from Validation import Validation
import zlib
import collections


# global variables
current_millis=int(round(time.time() * 1000,0))



class RTMInference:
    actualCurrentValue = 0
    currentPrediction = 0
    rawAnomalyScore = 0
    anomalyLikelihood = 0
    timestamp = 0


# implements the RTM algorithm used for KPIs
class LinearRegressionTemoporalMemory:
    #constructor
    def __init__(self, window, interval, min_, max_, boost, leak_detection, critical_region, debug):
        #holds the previous intervals
        self.x = deque()
        #holds the previous values
        self.y = deque()
        self.xv = []
        self.yv = []
        #how many iterations since startup
        self.iterations = 0
        #used for leak detection feature
        self.ascending = 0
        self.descending = 0
        #how many intervals to store for leak detection
        self.HISTORY_LENGTH = 3
        #window represents the number of steps in the past to consider
        self.window = window
        for i in range(0, self.window):
            self.x.append(0)
            self.y.append(0)
        #interval represents how far apart the steps are
        self.interval = interval
        #range of the signal
        self.min_ = min_
        self.max_ = max_
        #LR bias
        self.intercept = 0
        #LR slope
        self.slope = 0
        #sensitivity
        self.boost = boost
        #leak detection enabled
        self.leak_detection = leak_detection
        #critical region. right_tail, left_tail or two_tails
        self.critical_region = critical_region
        #debug enabled
        self.debug = debug
        #store anomaly history
        self.history = {}
        self.previousValue = 0
        #store error history
        self.deltaHistory = deque()
        for i in range(0, self.window*self.HISTORY_LENGTH):
            self.deltaHistory.append(0)
        self.rtm_inference = RTMInference()
        self.actualCurrentValue = 0
        self.currentPrediction = 0
        self.rawAnomalyScore = 0
        self.anomalyLikelihood = 0
        self.allzero = True
        self.run_logger = None
        self._init_logging()

    #computes the relative range of the signal from the history of values y
    def getRangeFromHistory(self):
        range_ = max(self.y)-min(self.y)
        if range_>0:
            return range_
        return 100

    def getMeanFromHistory(self):
        return sum(self.y)/len(self.y)

    def pushDeltaHistory(self, height):
        self.deltaHistory.popleft()
        self.deltaHistory.append(height)

    def checkAnomalyHistory(self, height):
        found = 0
        delta_min = height - 3
        delta_max = height + 3
        for item in list(self.deltaHistory):
            if item>0 and item>delta_min and item<delta_max:
                found = 1
                break
        return found

    def retrainRegressionModel(self):
        n = len(self.x)
        sumx = sum(self.x)
        sumy = sum(self.y)
        x_avg = sumx / n
        y_avg = sumy / n
        xx_sum = 0.0
        xy_sum = 0.0
        for i in range(0, n):
            xx_sum += (self.x[i] - x_avg) * (self.x[i] - x_avg)
            xy_sum += (self.x[i] - x_avg) * (self.y[i] - y_avg)
        self.slope  = xy_sum / xx_sum
        self.intercept = y_avg - self.slope * x_avg

    def predict(self, xval):
        return max(self.slope*xval + self.intercept, 0)

    def getNormalizedAnomalyScore(self, rawAnomalyScore):
        anomalyLikelihood = 0
        if rawAnomalyScore>0.9:
            anomalyLikelihood = 1
        elif rawAnomalyScore>0.70:
            anomalyLikelihood = 0.66
        else:
            anomalyLikelihood = 0
        return anomalyLikelihood

    def updateLeakDetection(self):
        if self.slope > 0:
            self.ascending = self.ascending + 1
            self.dsescending = 0
        if self.slope < 0:
            self.ascending = 0
            self.descending = self.descending + 1
        if self.slope == 0:
            self.ascending = 0
            self.descending = 0

    def getMaxFromAnomalyHistory(self, window_millis):
        max_from_history = 0
        #current_millis = int(round(time.time() * 1000,0))
        for millis in self.history.keys():
            if millis > (current_millis - window_millis):
                if self.history[millis] > max_from_history:
                    max_from_history = self.history[millis]
        return max_from_history

    def getEarliestAnomalyMillis(self, window_millis, threshold):
        millis=[]
        anomaly_logger.debug('window_millis: '+str(window_millis))
        anomaly_logger.debug('threshold: '+str(threshold))
        anomaly_logger.debug('self.history '+str(self.history))
        for ms, likelihood in self.history.items():
            if likelihood >= threshold:
                millis.append(ms)
        if millis:
            anomaly_logger.debug('return min(millis): '+str(min(millis)))
            return min(millis)
        else:
            return None

    def push_refactor(self, timestm, value):
        #increment iterations
        if value != 0:
            self.allzero=False
        else:
            if self.allzero==True:
                pass


        self.iterations = self.iterations + 1
        if not self.allzero:
            actualCurrentValue = float(value)
            self.run_logger.debug("RTM DEBUG::: actualCurrentValue=%s",actualCurrentValue)
            #get the predicted value from the model
            currentPrediction = self.predict(0)
            self.run_logger.debug("RTM DEBUG::: currentPrediction=%s", currentPrediction)
            self.run_logger.debug("RTM DEBUG::: history before shifting");
            for i in range(0, self.window):
                self.run_logger.debug("RTM DEBUG::: x[%s]=%s  y[%s]=%s", i, self.x[i], i, self.y[i] )
            #shift to the left (lose oldest value)
            for i in range(0, self.window-1):
                self.x[i] = self.x[i+1] - self.interval
            self.x[-1] = 0
            self.y.popleft()
            self.y.append(actualCurrentValue)
            #assign new actual value to the current position
            self.run_logger.debug("RTM DEBUG::: history after shifting");
            for i in range(0, self.window):
                self.run_logger.debug("RTM DEBUG::: x[%s]=%s  y[%s]=%s", i, self.x[i], i, self.y[i] )
            #retrain the RTM model
            self.retrainRegressionModel()
            self.run_logger.debug("RTM DEBUG::: After retrain slope=%s", self.slope)
            self.run_logger.debug("RTM DEBUG::: After retrain intercept=%s", self.intercept)
            #compare the currentPrediction with the actualCurrentValue
            if self.critical_region == "two_tails":
                delta = abs(currentPrediction - actualCurrentValue)
            elif self.critical_region == "left_tail":
                delta = currentPrediction - actualCurrentValue
            else: #treat as right_tail by default
                delta = actualCurrentValue - currentPrediction
            self.run_logger.debug("RTM DEBUG::: delta_adjusted=%s",delta)
            #find how far is based on the range (this should be between 0 and 1)
            #boost //1 low , 2 medium , 3 high sensitivity
            errorPercent = delta*(self.boost+5)/(self.max_-self.min_);
            self.run_logger.debug("RTM DEBUG::: After retrain errorPercent=%s",errorPercent)
            if errorPercent < 0: errorPercent=0
            #compute the raw anomaly score
            rawAnomalyScore = 0
            if abs(errorPercent)>0:
                #modified sigmoid
                try:
                    rawAnomalyScore = 1/(1+math.exp(-(40*errorPercent-10)))
                except OverflowError, Argument:
                    err_logger.error("math range error. errorPercent: "+str(errorPercent))
                    err_logger.error("math range error. errorPercent: "+str(errorPercent))
                    sys.exit(1)
            self.run_logger.debug("RTM DEBUG::: After retrain rawAnomalyScore=%s",rawAnomalyScore)
            if self.iterations < self.window:
                anomalyLikelihood = 0
            else:
                anomalyLikelihood = self.getNormalizedAnomalyScore(rawAnomalyScore)
            mean = self.getMeanFromHistory()
            if anomalyLikelihood>0:
                found = self.checkAnomalyHistory(mean + delta)
                self.pushDeltaHistory(mean+delta)
                if found>0:
                    #we found it in the history, so we're not signaling it again
                    anomalyLikelihood = 0
            else:
                #no anomaly, so setting 0 in history
                self.pushDeltaHistory(0)
            # check the leak detection
            if self.ascending>3*self.window and self.leak_detection==1:
                anomalyLikelihood = 1
                self.run_logger.debug("RTM DEBUG::: Ascending LEAK detected. Setting anomaly to 1")
                self.ascending=0
            # on descending side set it to 0.66 only
            if self.descending>3*self.window and self.leak_detection==1:
                anomalyLikelihood = 0.66
                self.run_logger.debug("RTM DEBUG::: Descending PATTERN detected. Setting anomaly to 0.66")
                self.descending = 0
            #update the leak detection with new values based on new slope
            self.updateLeakDetection()
            #add current reading to history
            #current_millis = time.time() * 1000
            self.history[current_millis] = anomalyLikelihood
            #remove older than 1 day
            for millis in self.history.keys():
                if millis < (current_millis - 60*60*1000):
                    del self.history[millis]
            print(anomalyLikelihood)
            self.run_logger.debug("RTM DEBUG::: After retrain anomalyLikelihood=%s",anomalyLikelihood);
            self.actualCurrentValue = actualCurrentValue
            self.currentPrediction = currentPrediction
            if self.iterations < self.window:
                self.rawAnomalyScore = 0
                self.anomalyLikelihood = 0
            else:
                self.rawAnomalyScore = rawAnomalyScore
                self.anomalyLikelihood = anomalyLikelihood
        else: # all-zero rtm, most lists/queues are unchanged with all zeros
            self.history[current_millis] = self.anomalyLikelihood
            for millis in self.history.keys():
                if millis < (current_millis - 60*60*1000):
                    del self.history[millis]

    def _init_logging(self):
        cur_timestamp = datetime.datetime.fromtimestamp(int(time.time())).strftime('%Y%m%d_%H%M%S')
        file_name = "rtm_on_ps_count_" + cur_timestamp + ".log" 
        #logging.basicConfig(stream=sys.stderr, level=logging.DEBUG)
        # logging.basicConfig(stream=sys.stderr, level=logging.INFO)
        logging.basicConfig(filename = os.path.join(os.getcwd(), file_name), level = logging.DEBUG, filemode = 'a', format = '%(asctime)s - %(levelname)s:\
    %(message)s')
        self.run_logger = logging.getLogger("requests")
        self.run_logger.setLevel(logging.WARNING)

    def analyze(self, ps_dict_unsorted):
        ps_od = collections.OrderedDict(sorted(ps_dict_unsorted.items()))
        keys, values = zip(*ps_od.items())
        
        iteration=1
        for i, timestamp in enumerate(ps_od):
            ps_count = ps_od[timestamp]
            print(ps_count)
            current_millis=int(round(time.time() * 1000,0))
            self.push_refactor(current_millis , ps_count)
            iteration+=1

def main():
    rtm = LinearRegressionTemoporalMemory(10, 10, 0, 600, 2, 0, "right_tail", 0)
    rtm.analyze({'1':'200', '2':'201', '3':'205', '4':'1010', '5':'200'})

if __name__ == "__main__":
    main()
