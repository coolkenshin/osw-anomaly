#!/usr/bin/env python
# ----------------------------------------------------------------------
# Numenta Platform for Intelligent Computing (NuPIC)
# Copyright (C) 2013, Numenta, Inc.  Unless you have an agreement
# with Numenta, Inc., for a separate license for this software code, the
# following terms and conditions apply:
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License version 3 as
# published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see http://www.gnu.org/licenses.
#
# http://numenta.org/licenses/
# ----------------------------------------------------------------------

"""
A simple client to run CLA anomaly detection using the OPF.
"""

from optparse import OptionParser
import sys
import csv
import math
import datetime
import dateutil.parser
import json
import collections
import plotly.plotly as py
import plotly.graph_objs as go
from oswdata_ps import OSWData, PS
from rtm import LinearRegressionTemoporalMemory

from nupic.frameworks.opf.modelfactory import ModelFactory
from nupic.frameworks.opf.predictionmetricsmanager import MetricsManager

from nupic.algorithms.anomaly_likelihood import AnomalyLikelihood

"""
Global variables
"""
""" Dictionary: timestamp: data """
g_ps_count_dict_unsorted = {}
g_abnomal_data_dict_unsorted={}

def runAnomaly(options):
    global g_ps_count_dict_unsorted
    global g_abnomal_data_dict_unsorted
    
    """
    Create and run a CLA Model on the given dataset (based on the hotgym anomaly
    client in NuPIC).
    """
    # Load the model params JSON
    with open("model_params.json") as fp:
        modelParams = json.load(fp)
    
    if options.oswpsDir != "":
        # Get PS dictionary
        osw = OSWData(options.oswpsDir, PS)
        osw.traverse_dir()
        g_ps_count_dict_unsorted = osw.get_ps_dict()
        options.max = ps_max_value = max(g_ps_count_dict_unsorted.values())
        options.min = ps_min_value = min(g_ps_count_dict_unsorted.values())
        print("Min value:" + str(ps_min_value) + ', ' + "Max value:" + str(ps_max_value))
    
    # Update the resolution value for the encoder
    sensorParams = modelParams['modelParams']['sensorParams']
    numBuckets = modelParams['modelParams']['sensorParams']['encoders']['value'].pop('numBuckets')
    resolution = options.resolution
    if resolution is None:
        resolution = max(0.001,
                       (options.max - options.min) / numBuckets)
    print("Using resolution value: {0}".format(resolution))
    sensorParams['encoders']['value']['resolution'] = resolution
    
    model = ModelFactory.create(modelParams)
    model.enableInference({'predictedField': 'value'})
    if options.inputFile != "":
        with open(options.inputFile) as fin:
            # Open file and setup headers
            # Here we write the log likelihood value as the 'anomaly score'
            # The actual CLA outputs are labeled 'raw anomaly score'
            reader = csv.reader(fin)
            csvWriter = csv.writer(open(options.outputFile, "wb"))
            csvWriter.writerow(["timestamp", "value",
                                "_raw_score", "likelihood_score", "log_likelihood_score"])
            headers = reader.next()
    
            # The anomaly likelihood object
            anomalyLikelihood = AnomalyLikelihood()
    
            # Iterate through each record in the CSV file
            print "Starting processing at", datetime.datetime.now()
            for i, record in enumerate(reader, start=1):
    
              # Convert input data to a dict so we can pass it into the model
              inputData = dict(zip(headers, record))
              inputData["value"] = float(inputData["value"])
              inputData["dttm"] = dateutil.parser.parse(inputData["dttm"])
              #inputData["dttm"] = datetime.datetime.now()
    
              # Send it to the CLA and get back the raw anomaly score
              result = model.run(inputData)
              anomalyScore = result.inferences['anomalyScore']
    
              # Compute the Anomaly Likelihood
              likelihood = anomalyLikelihood.anomalyProbability(
                  inputData["value"], anomalyScore, inputData["dttm"])
              logLikelihood = anomalyLikelihood.computeLogLikelihood(likelihood)
              if likelihood > 0.9999:
                print "Anomaly detected:", inputData['dttm'], inputData['value'], likelihood
    
              # Write results to the output CSV file
              csvWriter.writerow([inputData["dttm"], inputData["value"],
                                  anomalyScore, likelihood, logLikelihood])
    
              # Progress report
              if (i % 1000) == 0:
                print i, "records processed"
    elif options.oswpsDir != "":
        if options.use_rtm == True:
            rtm_sensitivity = 2
            rtm = LinearRegressionTemoporalMemory(window=10, interval=10, min_=options.min,
                                                  max_=options.max, boost=rtm_sensitivity,
                                                  leak_detection=0, critical_region="right_tail",
                                                  debug=0)
            g_abnomal_data_dict_unsorted = rtm.analyze(g_ps_count_dict_unsorted)
        else:
            csvWriter = csv.writer(open(options.outputFile, "wb"))
            csvWriter.writerow(["timestamp", "value",
                                "_raw_score", "likelihood_score", "log_likelihood_score"])
            ps_od = collections.OrderedDict(sorted(g_ps_count_dict_unsorted.items()))
        
            # The anomaly likelihood object
            anomalyLikelihood = AnomalyLikelihood()
        
            # Iterate through each record in the CSV file
            print "Starting processing at", datetime.datetime.now()
            for i, timestamp in enumerate(ps_od):
                ps_count = ps_od[timestamp]
                  
                inputData = {}
                inputData["value"] = float(ps_count)
                inputData["dttm"] = dateutil.parser.parse(timestamp)
                #inputData["dttm"] = datetime.datetime.now()
                
                # Send it to the CLA and get back the raw anomaly score
                result = model.run(inputData)
                anomalyScore = result.inferences['anomalyScore']
                
                # Compute the Anomaly Likelihood
                likelihood = anomalyLikelihood.anomalyProbability(
                    inputData["value"], anomalyScore, inputData["dttm"])
                logLikelihood = anomalyLikelihood.computeLogLikelihood(likelihood)
                if likelihood > 0.9999:
                    print "Anomaly detected:", inputData['dttm'], inputData['value'], likelihood
                    g_abnomal_data_dict_unsorted[timestamp] = ps_count
                
                # Write results to the output CSV file
                csvWriter.writerow([inputData["dttm"], inputData["value"],
                                    anomalyScore, likelihood, logLikelihood])
                
                # Progress report
                if (i % 1000) == 0:
                  print i, "records processed"


            print "Completed processing", i, "records at", datetime.datetime.now()
    print "Anomaly scores for", options.inputFile,
    print "have been written to", options.outputFile

def _plot_diagram(normal_unsorted_dict, abnomal_unsorted_dict, filename):
    normal_ps_od = collections.OrderedDict(sorted(normal_unsorted_dict.items()))
    normal_keys, normal_values = zip(*normal_ps_od.items())
    abnomal_ps_od = collections.OrderedDict(sorted(abnomal_unsorted_dict.items()))
    abnomal_keys, abnomal_values = zip(*abnomal_ps_od.items())
    
    normal_color = dict(color='#0000FF')
    abnomal_color = dict(color='#FF0000')

    trace_normal = go.Scatter(
        x = normal_keys,
        y = normal_values,
        mode = 'lines',
        marker=normal_color
    )
    
    trace_abnomal = go.Scatter(
        x = abnomal_keys,
        y = abnomal_values,
        mode = 'markers',
        marker=abnomal_color
    )

    data = [trace_normal, trace_abnomal]
    #py.iplot(data, filename=filename)
    py.plot(data, filename=filename)

def plot_diagram(options):
    if options.use_rtm == True:
        filename = 'ps_count_crond_problems_with_rtm_algorithm'
    else:
        filename = 'ps_count_crond_problems_with_htm_algorithm'
    _plot_diagram(g_ps_count_dict_unsorted, g_abnomal_data_dict_unsorted, filename)
    
if __name__ == "__main__":
    helpString = (
        "\n%prog [options] [uid]"
        "\n%prog --help"
        "\n"
        "\nRuns NuPIC anomaly detection on a csv file."
        "\nWe assume the data files have a timestamp field called 'dttm' and"
        "\na value field called 'value'. All other fields are ignored."
        "\nNote: it is important to set min and max properly according to data."
    )
    
    # All the command line options
    parser = OptionParser(helpString)
    parser.add_option("--inputFile",
                      help="Path to data file. (default: %default)",
                      dest="inputFile", default="")
    parser.add_option("--oswpsDir",
                      help="Path to oswpsDir. (default: %default)",
                      dest="oswpsDir", default="oswps")
    parser.add_option("--outputFile",
                      help="Output file. Results will be written to this file."
                      " (default: %default)",
                      dest="outputFile", default="default_output.csv")
    parser.add_option("--max", default=100.0, type=float,
                      help="Maximum number for the value field. [default: %default]")
    parser.add_option("--min", default=0.0, type=float,
                      help="Minimum number for the value field. [default: %default]")
    parser.add_option("--resolution", default=None, type=float,
                      help="Resolution for the value field (overrides min and max). [default: %default]")
    parser.add_option("-r", "--use_rtm",
                    action="store_false", dest="use_rtm", default=True,
                    help="Enabled RTM algorithm")
    
    options, args = parser.parse_args(sys.argv[1:])
    
    # Run it
    runAnomaly(options)
    plot_diagram(options)
  
  
