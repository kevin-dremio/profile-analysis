# This is a sample Python script.

# Press Shift+F10 to execute it or replace it with your code.
# Press Double Shift to search everywhere for classes, files, tool windows, actions, and settings.


import json
import math
import os
import datetime
import sys
import csv
import snappy
import pandas as pd
import fastparquet

#def getTimeDictStart(timeDict):
#    return timeDict.get('startEpochMs')


def process_files(input_directory, output_directory, tickSizeMs):
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
    profileQueryOutputPath = os.path.join(output_directory, 'queries.parquet')
    #queryFile = open(profileQueryOutputPath, 'w') #, newline='')
    #queryWriter = csv.DictWriter(queryFile, fieldnames=['qNum', 'queryId', 'userId', 'queueName', 'ruleName', 'queryCost', 'queryType', 'finalState',
    #                                                    'queryStartMs', 'queryEndMs', 'commandPoolWait', 'planningStartMs', 'planningEndMs',
    #                                                    'resourceSchedulingStartMs', 'resourceSchedulingEndMs', 'executionStartMs', 'executionEndMs',
    #                                                    'stateInvalidMs', 'statePendingMs', 'stateMetadataRetrievalMs', 'statePlanningMs', 'stateQueuedMs',
    #                                                    'stateEngineStartMs', 'stateExecutionPlanningMs', 'stateStartingMs', 'stateRunningMs',
    #                                                    'stateCompletedMs', 'stateCanceledMs', 'stateFailedMs'])
    #queryWriter.writeheader()

    profileNodesOutputPath = os.path.join(output_directory, 'nodes.parquet')
    #nodesFile = open(profileNodesOutputPath, 'w') #, newline='')
    #nodesWriter = csv.DictWriter(nodesFile, fieldnames=['qNum', 'endpointAddress', 'endpointFabricPort', 'maxMemoryUsed', 'enqueuedBeforeSubmitMs'])
    #nodesWriter.writeheader()

    profileTicksOutputPath = os.path.join(output_directory, 'ticks.parquet')
    #ticksFile = open(profileTicksOutputPath, 'w') #, newline='')
    #ticksWriter = csv.DictWriter(ticksFile, fieldnames=['tickMs', 'qNum', 'queryState', 'stateDurationMs'])
    #ticksWriter.writeheader()

    queryList = []  #pd.DataFrame()
    ticksList = []  #pd.DataFrame()
    nodesList = []  #pd.DataFrame()

    #ticksDF = pd.DataFrame()
    queryNumber = 0
    nrTicks = 0
    for profileFile in os.listdir(input_directory):
        if profileFile.endswith(".json"):
            profileFileName = os.path.basename(profileFile)
            parsedQueryId = profileFileName[8:-5]
            #print(parsedQueryId)
            #print(profileFileName)
            profileFilePath = os.path.join(input_directory, profileFile)

            data = [json.loads(line) for line in open(profileFilePath, 'r')]

            for profile in data:
                queryNumber = queryNumber + 1
                print(queryNumber)
                #print(profile)
                queryOut={}
                # queryOut['queryStartTs']=datetime.datetime.fromtimestamp(profile['start']/1000.0)
                queryOut['qNum'] = queryNumber
                queryOut['queryId'] = parsedQueryId
                queryOut['userId'] = profile['user']
                queryOut['queryStartMs'] = profile['start']
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
                queryOut['queryTextFirstChunk'] = profile['query'][:1000]

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
                #tickRow = {}
                for i in range(0, highestState):  # note we don't include the terminal state here
                    if stateStartTimes[i] > 0:
                        # compute the tick range for this phase based on ticksize
                        phaseStartTick = math.floor(float(stateStartTimes[i])/float(tickSizeMs)) * tickSizeMs
                        phaseEndTick = math.floor(float(stateEndTimes[i])/float(tickSizeMs)) * tickSizeMs
                        for t in range(phaseStartTick, phaseEndTick + tickSizeMs, tickSizeMs):
                            tickRow={}
                            tickRow['tickMs'] = t
                            tickRow['qNum'] = queryNumber
                            tickRow['queryState'] = i
                            #if not(i == 2 or i == 3 or i == 8):
                            #        print ('got one! ', i)
                            tickRow['stateDurationMs'] = stateDurations[i]
                            #print(tickRow)
                            nrTicks = nrTicks + 1

                            ticksList.append(tickRow)
                            #print(tickRow)

                queryList.append(queryOut)

                nodeProfile = profile['nodeProfile']
                if nodeProfile:
                    for node in nodeProfile:
                        nodeOut = {}
                        nodeOut['qNum'] = queryNumber
                        nodeOut['endpointAddress'] = node['endpoint']['address']
                        nodeOut['endpointFabricPort'] = node['endpoint']['fabricPort']
                        nodeOut['maxMemoryUsed'] = node['maxMemoryUsed']
                        nodeOut['enqueuedBeforeSubmitMs'] = node['timeEnqueuedBeforeSubmitMs']

                        nodesList.append(nodeOut)

    queryDF = pd.DataFrame(queryList)
    queryDF.sort_values(by=['queryStartMs'])
    queryDF.to_parquet(profileQueryOutputPath)

    nodesDF = pd.DataFrame(nodesList)
    nodesDF.to_parquet(profileNodesOutputPath)

    # WTF.  Loop thru the ticks list and count up how many of each state we have in the array
    #  then compare this to what we have in the dataframe
    stateCounts = [0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]
    for i in range(0, len(ticksList)):
        stateCounts[ticksList[i]['queryState']] += 1

    print (stateCounts)
    ticksDF = pd.DataFrame(ticksList)
    print('Total number of ticks processed ', nrTicks)
    #print('Length of Ticks list ', len(ticksList))
    print(ticksDF.groupby(['queryState']).count())
    ticksDF.to_parquet(profileTicksOutputPath)

if __name__ == '__main__':
    process_files("D:\\data\\process-profiles\\input\\backups\\dremio\\8bb9b52d-9f65-430b-b453-496dcf96f21f", "D:\\data\\process-profiles\\output", 100)
    #process_files("C:\\Users\\rjmay\\Downloads\\profiles\\backups\\dremio", "C:\\Users\\rjmay\\Downloads\\profiles\\parsed")
