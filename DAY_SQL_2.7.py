# -*- coding: utf-8 -*-
"""
Created on Tue Jul 09 14:12:47 2019

@author: Victor
"""

import ibm_db

def connectDB():
    conn_str='database=BLUDB;hostname=dashdb-txn-sbox-yp-lon02-02.services.eu-gb.bluemix.net;port=50000;protocol=tcpip;uid=kbc96252;pwd=nlkgclt^hvhgx7q0'
    ibm_db_conn = ibm_db.connect(conn_str,'','')
    #conn = ibm_db_dbi.Connection(ibm_db_conn)
    return ibm_db_conn

# selects de la BD

def selectDB(params,table,connection):
    select="select "+params+" from "+table
    stmt_select = ibm_db.exec_immediate(connection, select)
    cols = ibm_db.fetch_tuple( stmt_select )
    print("Resultados: ")
    print("%s, %s" % (cols[0], cols[1]))

def insertDB(params,table,connection):
    insert = "insert into "+ table +" values(?,?)"
    stmt_insert = ibm_db.prepare(connection, insert)
    ibm_db.execute_many(stmt_insert,params)

#nos conectamos a la DB
conexion = connectDB()

#hacemos una consulta SQL para sacar los datos de la tabla TEST de nuestra BD
parametros = 'DNI, NOMBRE'
tabla      = 'TEST'
selectDB(parametros,tabla,conexion)

# OPTATIVO, hacer un insert (tarda un poco a veces en procesarlo)
params=((2,'Jesus'),(3,'Oriol'),(4,'Francesc'))
insertDB(params,tabla,conexion)

# comprobamos que ha ido todo bien
selectDB(parametros,tabla,conexion)

ibm_db.close(conexion)
