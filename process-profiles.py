import json
import math
import os
import datetime
import sys
import snappy
import pandas as pd
# import fastparquet
import pyarrow
import time

def getTimeDictStart(timeDict):
    return timeDict.get('startEpochMs')

def process_files(input_directory, output_directory, tickSizeMs, queryChunkSize):
    stateName = ['INVALID_STATE',
                 'PENDING',
                 'METADATA_RETRIEVAL',
                 'PLANNING',
                 'QUEUED',
                 'ENGINE_START',
                 'EXECUTION_PLANNING',
                 'STARTING',
                 'RUNNING',
                 'COMPLETED',
                 'CANCELED',
                 'FAILED'
                 ]

    stateTimingName =  ['stateInvalidMs',
                        'statePendingMs',
                        'stateMetadataRetrievalMs',
                        'statePlanningMs',
                        'stateQueuedMs',
                        'stateEngineStartMs',
                        'stateExecutionPlanningMs',
                        'stateStartingMs',
                        'stateRunningMs',
                        'stateCompletedMs',
                        'stateCanceledMs',
                        'stateFailedMs'
                        ]


    print(input_directory)

    queryList = []
    ticksList = []
    nodesList = []
    queryTextChunks = []

    nrTicks = 0

    # Initial attempt at creating a bigint-storable surrogate key for each query,
    #   which stores much smaller than UUID (especially the text version of UUID)
    currentEpoch10s = math.floor(time.time()/10)   # here we assume that this utility won't be run more frequently than every 10s
    queryNumber = currentEpoch10s * 10000000  # and that each run of this tool will not process more than 10 million query profiles

    firstQueryStartMs = 0
    lastQueryStartMs = 0
    # print (queryNumberOffset)
    for profileFile in os.listdir(input_directory):
        if profileFile.endswith(".JSON"):   # the query dump directory will have .json and .crc files, we only want the .json ones
            profileFileName = os.path.basename(profileFile)   # queryID is written into the filename itself, but is NOT in the profile data
            parsedQueryId = profileFileName[8:-5]
            profileFilePath = os.path.join(input_directory, profileFile)

            data = [json.loads(line) for line in open(profileFilePath, 'r')]   # load the json for a single file

            for profile in data:    # loop over it, in the event we have multiple profiles associated with a single queryID.
                                    #  This can happen with multiple attempts    //TODO: we need to handle multi-attempts better

                queryNumber = queryNumber + 1
                print(parsedQueryId)
                queryOut={} # create an empty dictionary and start building up the queryHeader object
                queryOut['queryId'] = parsedQueryId
                queryOut['qNum'] = queryNumber
                queryOut['userId'] = profile['user']

                queryStartMs = profile['start']
                if firstQueryStartMs == 0 or queryStartMs < firstQueryStartMs:
                    firstQueryStartMs = queryStartMs
                if lastQueryStartMs ==0 or queryStartMs > lastQueryStartMs:
                    lastQueryStartMs = queryStartMs

                queryOut['queryStartMs'] = queryStartMs
                queryOut['commandPoolWait'] = profile['commandPoolWaitMillis']
                queryOut['totalFragments'] = profile['totalFragments']
                queryOut['finishedFragments'] = profile['finishedFragments']
                if profile['planningStart']:
                    queryOut['planningStartMs'] = profile['planningStart']

                try:
                    queryOut['planningEndMs'] = profile['planningEnd']
                except:
                    queryOut['planningEndMs'] = None

                try:
                    queryOut['resourceSchedulingStartMs'] = profile['resourceSchedulingProfile']['resourceSchedulingStart']
                    queryOut['resourceSchedulingEndMs'] = profile['resourceSchedulingProfile']['resourceSchedulingEnd']
                    queryOut['queueName'] = profile['resourceSchedulingProfile']['queueName']
                    queryOut['ruleName'] = profile['resourceSchedulingProfile']['ruleName']
                    queryOut['queryCost'] = int(round(profile['resourceSchedulingProfile']['schedulingProperties']['queryCost']))
                    queryOut['queryType'] = profile['resourceSchedulingProfile']['schedulingProperties']['queryType']
                except:
                    queryOut['resourceSchedulingStartMs'] = None
                    queryOut['resourceSchedulingEndMs'] = None
                    queryOut['queueName'] = None
                    queryOut['ruleName'] = None
                    queryOut['queryCost'] = None
                    queryOut['queryType'] = None
                    #print('no resourceSchedulingProfile')

                try:
                    queryOut['executionStartMs'] = profile['executionStart']
                except:
                    queryOut['executionStartMs'] = None
                    #print('no ExecutionStart')

                queryOut['queryEndMs'] = profile['end']
                queryText = profile['query']
                queryOut['queryTextFirstChunk'] = queryText[:queryChunkSize]  # Write the first query text chunk into the queryOut header row

                # and then normalize all query chunks out into a separate table, in the event we need to reconstruct the entire query text for very large
                #   queries.
                lenQueryText = len(queryText)
                nrChunks = math.ceil(lenQueryText/queryChunkSize)

                queryOut['nrQueryChunks'] = nrChunks
                # print (queryText)
                if lenQueryText > 0:
                    for i in range(0, nrChunks):
                        queryTextChunk = {}
                        queryTextChunk['qNum'] = queryNumber
                        queryTextChunk['chunkNum'] = i
                        startOffset = i * queryChunkSize
                        endOffset = (i+1) * queryChunkSize
                        queryTextChunk['chunkText'] = queryText[startOffset:endOffset]
                        # print (i, startOffset, endOffset, queryTextChunk)
                        queryTextChunks.append(queryTextChunk)

                # determine the start and end millisecond timestamps and durations for every phase the query passed through
                #   also determine the final end-state of the query
                highestState = 0
                stateStartTimes = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
                stateStartTicks = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
                stateEndTimes = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
                stateDurations = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
                stateList = profile['stateList']
                for state in stateList:
                    stateCode = state['state']
                    stateStartMs = state['startTime']
                    if stateCode > highestState:
                        highestState = stateCode

                    stateStartTimes[stateCode] = stateStartMs
                    stateStartTicks[stateCode] = math.floor(float(stateStartMs)/float(tickSizeMs)) * tickSizeMs

                    queryOut[stateTimingName[stateCode]] = stateStartMs

                queryOut['finalState'] = stateName[highestState]

                # compute start times, end times, and durations for each phase
                prevState = 0
                for i in range(0, highestState + 1):
                    if stateStartTimes[i] > 0:
                        if prevState > 0:
                            stateEndTimes[prevState] = stateStartTimes[i]
                            stateDurations[prevState] = stateStartTimes[i] - stateStartTimes[prevState]
                        prevState = i
                stateEndTimes[prevState] = stateStartTimes[i]
                stateDurations[prevState] = stateStartTimes[i] - stateStartTimes[prevState]

                # now expand the phase data into tick data, by stepping thru the timing arrays
                #   and writing records for each tick between start/end
                for i in range(0, highestState):  # note we don't include the terminal state here
                    if stateStartTimes[i] > 0:
                        # compute the tick range for this phase based on ticksize
                        phaseStartTick = math.floor(float(stateStartTimes[i])/float(tickSizeMs)) * tickSizeMs
                        phaseEndTick = math.floor(float(stateEndTimes[i])/float(tickSizeMs)) * tickSizeMs
                        for t in range(phaseStartTick, phaseEndTick + tickSizeMs, tickSizeMs):
                            tickRow={}
                            tickRow['tickMs'] = t
                            tickRow['qNum'] = queryNumber
                            # tickRow['queryId'] = parsedQueryId
                            tickRow['queryState'] = i
                            tickRow['stateDurationMs'] = stateDurations[i]
                            #print(tickRow)
                            nrTicks = nrTicks + 1

                            ticksList.append(tickRow)

                queryList.append(queryOut)  # add query header row to the queries array

                # if the profile shows that the query was processed on executors, then extract the executor nodes
                #   which were involved in the query, and normalize this into a separate 'nodes' table.
                # For each node, grab the host and port of the executor process, as well as the high-water mark for RAM
                nodeProfile = profile['nodeProfile']
                if nodeProfile:
                    for node in nodeProfile:
                        nodeOut = {}
                        nodeOut['qNum'] = queryNumber
                        # nodeOut['queryId'] = parsedQueryId
                        nodeOut['endpointAddress'] = node['endpoint']['address']
                        nodeOut['endpointFabricPort'] = node['endpoint']['fabricPort']
                        nodeOut['maxMemoryUsed'] = node['maxMemoryUsed']
                        nodeOut['enqueuedBeforeSubmitMs'] = node['timeEnqueuedBeforeSubmitMs']

                        nodesList.append(nodeOut)


    # This utility is meant to be run repeatedly (daily, or multiple times per day) on profile exports from Dremio
    #   as such, each run needs to append new data to existing parquet tables.  We accomplish this by having each output
    #   table live in separate subdirectory, with new files for each table written into the appropriate directory

    # As an attempt to prevent duplication if the script is run again against the same input data, we name the files
    #   with the start millisecond timestamps of the first and last queries seen in the input set.  Will need to come up
    #   with a smarter scheme for this in the next refactor
    fileNameMsRange = str(firstQueryStartMs) + "_" + str(lastQueryStartMs)

    # check if the output directory exists; if not, create it, and set the filename for the output file to include the
    #   ms time range computed above
    # //TODO: refactor these repeated calls into a function
    queryOutputPath = os.path.join(output_directory, 'queries')
    if not os.path.exists(queryOutputPath):
        os.makedirs(queryOutputPath)
    queryOutputPath = os.path.join(queryOutputPath, 'queries_' + fileNameMsRange + '.parquet')

    queryTextOutputPath = os.path.join(output_directory, 'query_text')
    if not os.path.exists(queryTextOutputPath):
        os.makedirs(queryTextOutputPath)
    queryTextOutputPath = os.path.join(queryTextOutputPath, 'query_text_' + fileNameMsRange + '.parquet')

    nodesOutputPath = os.path.join(output_directory, 'nodes')
    if not os.path.exists(nodesOutputPath):
        os.makedirs(nodesOutputPath)
    nodesOutputPath = os.path.join(nodesOutputPath, 'nodes_' + fileNameMsRange + '.parquet')

    ticksOutputPath = os.path.join(output_directory, 'ticks')
    if not os.path.exists(ticksOutputPath):
        os.makedirs(ticksOutputPath)
    ticksOutputPath = os.path.join(ticksOutputPath, 'ticks_' + fileNameMsRange + '.parquet')

    cpusOutputPath = os.path.join(output_directory, 'cpus')
    if not os.path.exists(cpusOutputPath):
        os.makedirs(cpusOutputPath)
    cpusOutputPath = os.path.join(cpusOutputPath, 'cpu_' + fileNameMsRange + '.parquet')


    # write query header, query text, and nodes arrays to dataframes.  Sort as applicable, and write to parquet
    queryDF = pd.DataFrame(queryList)
    queryDF.sort_values(by=['queryStartMs'])
    queryDF.to_parquet(queryOutputPath)

    queryTextDF = pd.DataFrame(queryTextChunks)
    queryTextDF.to_parquet(queryTextOutputPath)

    nodesDF = pd.DataFrame(nodesList)
    nodesDF.to_parquet(nodesOutputPath)

    # Double-check the counts of query states and durations by looping thru the ticks list
    # and counting up stats in the array, then compare with what we have in the dataframe
    stateCounts = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    for i in range(0, len(ticksList)):
        stateCounts[ticksList[i]['queryState']] += 1
    print (stateCounts)

    # Write the ticks list to dataframe and write to parquet
    ticksDF = pd.DataFrame(ticksList)

    # Output final stats
    print('Total number of ticks processed ', nrTicks)
    print(ticksDF.groupby(['queryState']).count())
    ticksDF.to_parquet(ticksOutputPath)

if __name__ == '__main__':
    # inputDir = sys.argv[1]
    # outputDir = sys.argv[2]
    # timeResolution = sys.argv[3]
    # queryChunkSize = sys.argv[4]

    # process_files(inputDir, outputDir, timeResolution, queryChunkSize)  //TODO: replace this with argparse

    #process_files("D:\\data\\process-profiles\\input\\8bb9b52d-9f65-430b-b453-496dcf96f21f", "D:\\data\\process-profiles\\output", 10, 1000)  # hardcode for testing
    process_files("C:\\Users\Kevin Lambrecht\\Documents\\Customers\\JPMC\\CS-9647\\profiles\\tmp\\profiles\\20210426205243_369", "C:\\Users\Kevin Lambrecht\\Documents\\Customers\\JPMC\\CPU_output", 100, 1000)