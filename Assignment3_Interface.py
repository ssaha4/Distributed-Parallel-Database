#!/usr/bin/python2.7
#
# Assignment3 Interface
#

import psycopg2
import os
import sys
import threading

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    cur = openconnection.cursor()  
    droptablequery = "DROP TABLE IF EXISTS " + OutputTable + ";"
    cur.execute(droptablequery)

    createtablequery = "CREATE TABLE " + OutputTable + " AS SELECT * FROM " + InputTable + " WHERE 1 = 2;"
    cur.execute(createtablequery) 

    getminmaxquery = "SELECT MIN(" + SortingColumnName + "), MAX(" + SortingColumnName + ")  from " + InputTable + ";"
    cur.execute(getminmaxquery)
    minrange, maxrange = cur.fetchone()
    partitioninterval = round((maxrange - minrange)/5,2)

    threads = [0] * 5
    for i in range(5):
        partitiontable = "partition" + str(i)
        minval = minrange + i * partitioninterval
        maxval = minval + partitioninterval
        
        threads[i] = threading.Thread(target = sortFunction, args=(InputTable, SortingColumnName,partitiontable, minval, maxval, openconnection, i))
        threads[i].start()
    
    
    for i in range(5):
        threads[i].join()
        partitiontable = "partition" + str(i)
        inserttablequery = "INSERT INTO " + OutputTable + " SELECT * FROM " + partitiontable + ";"
        cur.execute(inserttablequery)
        droptemptable  = "DROP TABLE IF EXISTS " + partitiontable + ";"
        cur.execute(droptemptable)
    cur.close()
    openconnection.commit()

def sortFunction(InputTable,SortingColumnName,partitiontable, minval, maxval, openconnection, threadindex):
    cur = openconnection.cursor()
    droptablequery = "DROP TABLE IF EXISTS " + partitiontable + ";"
    cur.execute(droptablequery)
    createtablequery = ""
                                
    if threadindex == 0:
        createtablequery = "CREATE TABLE " + partitiontable + " AS SELECT * FROM " + InputTable + " WHERE " + SortingColumnName + " >= " + str(minval) + " AND " + SortingColumnName + " <= " + str(maxval) + " ORDER BY " + SortingColumnName + " ASC;"
    else:
        createtablequery = "CREATE TABLE " + partitiontable + " AS SELECT * FROM " + InputTable + " WHERE " + SortingColumnName + " > " + str(minval) + " AND " + SortingColumnName + " <= " + str(maxval) + " ORDER BY " + SortingColumnName + " ASC;"
    
    cur.execute(createtablequery)
    openconnection.commit()

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    
    cur = openconnection.cursor()  
    droptablequery = "DROP TABLE IF EXISTS " + OutputTable + ";"
    cur.execute(droptablequery)

    createtablequery = "CREATE TABLE " + OutputTable + " AS SELECT "+ InputTable1 + ".* , " + InputTable2 + ".* FROM " + InputTable1 + " , " + InputTable2 + " WHERE 1 = 2;"
    cur.execute(createtablequery)
    
    getminmaxquerytable1 = "SELECT MIN(" + Table1JoinColumn + "), MAX(" + Table1JoinColumn + ")  from " + InputTable1 + ";"
    cur.execute(getminmaxquerytable1)
    minrange1, maxrange1 = cur.fetchone()
    
    getminmaxquerytable2 = "SELECT MIN(" + Table2JoinColumn + "), MAX(" + Table2JoinColumn + ")  from " + InputTable2 + ";"
    cur.execute(getminmaxquerytable2)
    minrange2, maxrange2 = cur.fetchone()
    
    minrange = min(minrange1, minrange2)
    maxrange = max(maxrange1, maxrange2)
    
    partitioninterval = round((maxrange - minrange)/5,2)
    threads = [0] * 5
    for i in range(5):
        partitiontable1 = "tb1partition" + str(i)
        partitiontable2 = "tb2partition" + str(i)
        minval = minrange + i * partitioninterval
        maxval = minval + partitioninterval
        
        threads[i] = threading.Thread(target = joinFunction, args=(InputTable1, InputTable2,Table1JoinColumn, Table2JoinColumn,partitiontable1,partitiontable2,minval, maxval, openconnection,i))
        threads[i].start()
        
    for i in range(5):
        threads[i].join()
        partitiontable1 = "tb1partition" + str(i)
        partitiontable2 = "tb2partition" + str(i)
        inserttablequery = "INSERT INTO " + OutputTable + " SELECT * FROM " + partitiontable1 + " INNER JOIN " + partitiontable2 + " ON " + partitiontable1 + "." + Table1JoinColumn + " = " + partitiontable2 + "." + Table2JoinColumn + ";"
        cur.execute(inserttablequery)
        droptemptable1  = "DROP TABLE IF EXISTS " + partitiontable1 + ";"
        cur.execute(droptemptable1)
        droptemptable2  = "DROP TABLE IF EXISTS " + partitiontable2 + ";"
        cur.execute(droptemptable2)
    cur.close()
    openconnection.commit()
    

def joinFunction(InputTable1, InputTable2,Table1JoinColumn,Table2JoinColumn, partitiontable1, partitiontable2, minval, maxval, openconnection,threadindex):
    
    cur = openconnection.cursor()
    droptable1query = "DROP TABLE IF EXISTS " + partitiontable1 + ";"
    cur.execute(droptable1query)
    droptable2query = "DROP TABLE IF EXISTS " + partitiontable2 + ";"
    cur.execute(droptable2query)
    
    createtable1query = ""
    createtable2query = ""
    
    if threadindex == 0:
        createtable1query = "CREATE TABLE " + partitiontable1 + " AS SELECT * FROM " + InputTable1 + " WHERE " + Table1JoinColumn + " >= " + str(minval) + " AND " + Table1JoinColumn + " <= " + str(maxval) + ";"
        createtable2query = "CREATE TABLE " + partitiontable2+ " AS SELECT * FROM " + InputTable2 + " WHERE " + Table2JoinColumn + " >= " + str(minval) + " AND " + Table2JoinColumn + " <= " + str(maxval) + ";"
    else:
        createtable1query = "CREATE TABLE " + partitiontable1 + " AS SELECT * FROM " + InputTable1 + " WHERE " + Table1JoinColumn + " > " + str(minval) + " AND " + Table1JoinColumn + " <= " + str(maxval) + ";"
        createtable2query = "CREATE TABLE " + partitiontable2 + " AS SELECT * FROM " + InputTable2 + " WHERE " + Table2JoinColumn + " > " + str(minval) + " AND " + Table2JoinColumn + " <= " + str(maxval) + ";"
    
    cur.execute(createtable1query)
    cur.execute(createtable2query)
    openconnection.commit()
