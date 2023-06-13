#!/usr/bin/python3

import numpy as np
import operator
import re
import sys 
import cv2
import glob
from os import listdir

# linear regression
# 매개변수로 받은 디렉토리들을 각각 train_data, test_data에 저장

train_data = sys.argv[1]
test_data = sys.argv[2]

def generateVector(file):
    vector = np.zeros( (1,1024) )
    fr = open(file)
    for i in range(32):
        line = fr.readline()
        for j in range(32):
              vector[0,32*i+j] = int(float(line[j]))
    return vector

# def createDataSet():
#     group = np.array([[1.0, 1.1], [1.0, 1.0], [0, 0], [0, 0.1]])
#     labels = ['A', 'A', 'B', 'B']
#     return group, labels

def autoNorm(dataSet):
    minVals = dataSet.min(0)
    maxVals = dataSet.max(0)
    ranges = maxVals - minVals
    normDataSet = np.zeros(np.shape(dataSet))
    m = dataSet.shape[0]
    normDataSet = dataSet - np.tile(minVals, (m, 1))
    normDataSet = normDataSet / np.tile(ranges, (m, 1))
    return normDataSet, ranges, minVals

def classify0(inX, dataSet, labels, k):
    dataSetSize = dataSet.shape[0]
    diffMat = np.tile(inX, (dataSetSize, 1)) - dataSet
    sqDiffMat = diffMat ** 2
    sqDistances = sqDiffMat.sum(axis = 1)
    distances = sqDistances ** 0.5
    sortedDistIndicies = distances.argsort()
    classCount = {}
    for i in range(k):
        voteIlabel = labels[sortedDistIndicies[i]]
        classCount[voteIlabel] = classCount.get(voteIlabel, 0) + 1
    sortedClassCount = sorted(classCount.items(),
            key = operator.itemgetter(1), reverse = True)
    return sortedClassCount[0][0]
        
def errorCheck(k):
    errorCount = 0.0

    # 디렉토리 파일들을 List로 저장
    trainDataSet = listdir(train_data)
    testDataSet = listdir(test_data)

    m = len(trainDataSet)
    n = len(testDataSet)
    train_matrix = np.zeros( (m,1024) )
    
    labels = []
    
    for i in range(m):
        file = trainDataSet[i]
        file_name = file.split(".")[0]
        answer = int(file_name.split("_")[0])
        labels.append(answer)
        train_matrix[i,:] = generateVector("%s/%s" % (train_data, file))

    for i in range(n):
        file = testDataSet[i]
        file_name = file.split(".")[0]
        answer = int(file_name.split("_")[0])
        testVector = generateVector("%s/%s" % (test_data, file))
        result = classify0(testVector, train_matrix, labels, k)
        if (result != answer):
                errorCount += 1.0
    print( int(errorCount / n * 100) )
    

for i in range(20):
        errorCheck(i + 1)
