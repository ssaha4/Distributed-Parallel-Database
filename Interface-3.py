#!/usr/bin/python2.7
#
# Assignment2 Interface
#

import psycopg2
import os
import sys

RANGE_TABLE_PREFIX = 'RangeRatingsPart'
RROBIN_TABLE_PREFIX = 'RoundRobinRatingsPart'
RANGE_METADATA = 'RangeRatingsMetadata'
RROBIN_METADATA = 'RoundRobinRatingsMetadata'

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    textfile = 'RangeQueryOut.txt'
    if os.path.exists(textfile):
        os.remove(textfile)
    PartitionNum,MaxRating, MinRating  = [], [], []
    cur = openconnection.cursor()
    cur.execute("SELECT PartitionNum, MinRating, MaxRating FROM "+RANGE_METADATA)
    for r in cur.fetchall():
        PartitionNum.append(r[0])
        MinRating.append(r[1])
        MaxRating.append(r[2])
    
    for i, val in enumerate(PartitionNum):
        if ratingMaxValue >= MinRating[i] and ratingMinValue <= MaxRating[i]:
            range_table = RANGE_TABLE_PREFIX+str(val)
            cur.execute("SELECT * FROM "+range_table+" WHERE rating >= "+str(ratingMinValue)+" and rating <= "+str(ratingMaxValue))
            records = cur.fetchall()
            with open(textfile, 'a') as fl:
                for record in records:
                    fl.write(range_table + "," + ",".join(map(str, record)))
                    fl.write("\n")
    
    cur.execute("SELECT PartitionNum FROM "+RROBIN_METADATA)
    PartitionNum = cur.fetchone()[0]
    for i in range(PartitionNum):
        rrobin_table = RROBIN_TABLE_PREFIX+str(i)
        cur.execute("SELECT * FROM "+rrobin_table+" WHERE rating >= "+str(ratingMinValue)+" and rating <= "+str(ratingMaxValue))
        records = cur.fetchall()
        with open(textfile, 'a') as fl:
            for record in records:
                fl.write(rrobin_table + "," + ",".join(map(str, record)))
                fl.write("\n")
    
    cur.close()



def PointQuery(ratingsTableName, ratingValue, openconnection):
    textfile = 'PointQueryOut.txt'
    if os.path.exists(textfile):
        os.remove(textfile)
    PartitionNum, MinRating, MaxRating = [], [], []
    cur = openconnection.cursor()
    cur.execute("SELECT PartitionNum, MinRating, MaxRating FROM "+RANGE_METADATA)
    for r in cur.fetchall():
        PartitionNum.append(r[0])
        MinRating.append(r[1])
        MaxRating.append(r[2])
    for i ,val in enumerate(PartitionNum):
        if ratingValue >= MinRating[i] and ratingValue <= MaxRating[i]:
            range_table = RANGE_TABLE_PREFIX+str(val)
            cur.execute("SELECT * FROM "+range_table+" WHERE rating = "+str(ratingValue))
            records = cur.fetchall()
            with open(textfile, 'a') as fl:
                for record in records:
                    fl.write(range_table + "," + ",".join(map(str, record)))
                    fl.write("\n")
    
    cur.execute("SELECT PartitionNum FROM "+RROBIN_METADATA)
    PartitionNum = cur.fetchone()[0]
    for i in range(PartitionNum):
        rrobin_table = RROBIN_TABLE_PREFIX+str(i)
        cur.execute("SELECT * FROM "+rrobin_table+" WHERE rating = "+str(ratingValue))
        records = cur.fetchall()
        with open(textfile, 'a') as fl:
            for record in records:
                fl.write(rrobin_table + "," + ",".join(map(str, record)))
                fl.write("\n")
    cur.close()
    


def writeToFile(filename, rows):
    f = open(filename, 'w')
    for line in rows:
        f.write(','.join(str(s) for s in line))
        f.write('\n')
    f.close()
