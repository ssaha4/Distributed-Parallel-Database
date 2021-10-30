#!/usr/bin/python2.7
#
# Interface for the assignement
#

import psycopg2


def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
	return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
	cur = openconnection.cursor()
	cur.execute('DROP TABLE IF EXISTS '+ratingstablename)
	cur.execute("create table " + ratingstablename + "(userid integer, spcl_char1 char, movieid integer, spcl_char2 char, rating float, spcl_char3 char, timestamp bigint);")
	cur.copy_from(open(ratingsfilepath,'r'),ratingstablename,sep=':')
	cur.execute("alter table " + ratingstablename + " drop column spcl_char1, drop column spcl_char2, drop column spcl_char3, drop column timestamp;")
	openconnection.commit()
	cur.close()
	


def rangePartition(ratingstablename, numberofpartitions, openconnection):
	RANGE_TABLE_PREFIX = 'range_part'
	cur = openconnection.cursor()
	partitionInterval = 5/ numberofpartitions
	for i in range(numberofpartitions):
		range_part_tablename = RANGE_TABLE_PREFIX + str(i)
		cur.execute("DROP TABLE IF EXISTS " + range_part_tablename)
		cur.execute("create table " + range_part_tablename + " (userid integer, movieid integer, rating float);")
		lowerlimit = i*partitionInterval
		upperlimit = lowerlimit+partitionInterval
		if i == 0:
			cur.execute("insert into " + range_part_tablename + " (userid, movieid, rating) select userid, movieid, rating from " + ratingstablename + " where rating >= " + str(lowerlimit) + " and rating <= " + str(upperlimit) + ";")
		else:
			cur.execute("insert into " + range_part_tablename + " (userid, movieid, rating) select userid, movieid, rating from " + ratingstablename + " where rating > " + str(lowerlimit) + " and rating <= " + str(upperlimit) + ";")
	openconnection.commit()
	cur.close()

def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
	RROBIN_TABLE_PREFIX = 'rrobin_part'
	cur = openconnection.cursor()
	for i in range(numberofpartitions):
		rrobin_part_tablename = RROBIN_TABLE_PREFIX + str(i)
		cur.execute("DROP TABLE IF EXISTS " + rrobin_part_tablename)
		cur.execute("create table " +rrobin_part_tablename + " (userid integer, movieid integer, rating float);")
		cur.execute("insert into " + rrobin_part_tablename + " (userid, movieid, rating) select userid, movieid, rating from (select userid, movieid, rating, row_number() over() as rownum from  "+ ratingstablename +") as tmp where mod(tmp.rownum-1,"+str(numberofpartitions)+")="+str(i))   
	openconnection.commit()
	cur.close()


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):
	RROBIN_TABLE_PREFIX = 'rrobin_part'
	cur = openconnection.cursor()
	cur.execute("insert into " + ratingstablename + " (userid, movieid, rating) values("+str(userid)+","+str(itemid)+","+str(rating)+")")
	cur.execute("select count(*) from " + ratingstablename)
	totalrows = cur.fetchall()[0][0]
	cur.execute("select count(*) from pg_stat_user_tables where relname like " + "'" + RROBIN_TABLE_PREFIX + "%';")
	num_part_tables = cur.fetchone()[0]
	part_no = (totalrows-1)%num_part_tables
	rrobin_part_tablename = RROBIN_TABLE_PREFIX + str(part_no)
	cur.execute("insert into " + rrobin_part_tablename + " (userid, movieid, rating) values("+str(userid)+","+str(itemid)+","+str(rating)+")")
	openconnection.commit()
	cur.close()


def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
	RANGE_TABLE_PREFIX = 'range_part'
	cur = openconnection.cursor()
	cur.execute("insert into " + ratingstablename + " (userid, movieid, rating) values("+str(userid)+","+str(itemid)+","+str(rating)+")")
	cur.execute("select count(*) from " + ratingstablename)
	totalrows = cur.fetchall()[0][0]
	cur.execute("select count(*) from pg_stat_user_tables where relname like " + "'" + RANGE_TABLE_PREFIX + "%';")
	num_part_tables = cur.fetchone()[0]
	partitionInterval = 5/ num_part_tables
	i = index=0
	while(i<=5.0):
		range_part_tablename = RANGE_TABLE_PREFIX + str(index)
		if(i==0):
			if(rating>=i and rating<=(i+partitionInterval)):
				cur.execute("insert into " + range_part_tablename + " (userid, movieid, rating) values("+str(userid)+","+str(itemid)+","+str(rating)+")")
		else:
			if(rating>i and rating<=(i+partitionInterval)):
				cur.execute("insert into " + range_part_tablename + " (userid, movieid, rating) values("+str(userid)+","+str(itemid)+","+str(rating)+")")
		i += partitionInterval
		index+=1
	openconnection.commit()
	cur.close()

def createDB(dbname='dds_assignment'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print 'A database named {0} already exists'.format(dbname)

    # Clean up
    cur.close()
    con.close()

def deletepartitionsandexit(openconnection):
    cur = openconnection.cursor()
    cur.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
    l = []
    for row in cur:
        l.append(row[0])
    for tablename in l:
        cur.execute("drop table if exists {0} CASCADE".format(tablename))

    cur.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    except IOError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
    finally:
        if cursor:
            cursor.close()
