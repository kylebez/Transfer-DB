import sys
import arcpy
import pyodbc
import logging
from datetime import date
from time import gmtime, strftime, localtime
import traceback
import ConfigParser

config = ConfigParser.ConfigParser()
config.read(['config.cfg', os.path.expanduser('~/config.cfg')])

#logging variables
logging.basicConfig(filename=config.get("Logging","filename"), level=logging.INFO)

#odbc variable
db_host = config.get("ODBC", "db_host")
db_name = config.get("ODBC", "db_name")
db_user = config.get("ODBC", "db_user")
db_password = config.get("ODBC", "db_password")
connection_string = 'Driver={SQL Server};Server=' + db_host + ';Database=' + db_name + ';UID=' + db_user + ';PWD=' + db_password + ';'

#First Database Source
Table1 = config.get("Source/Destination Tables", "Source1")
#Second Database Source
Table2 = config.get("Source/Destination Tables", "Source2")

SrcIsNotGDB = False
DstIsNotGDB = False

def transferData(src, dest):   
    #concatenate src and dest paths and check editing capability
    global SrcIsNotGDB
    global DstIsNotGDB
    sourceTable = src
    destTable = dest
    if src == 'ODBC':
        SrcIsNotGDB = True
    else:
        srcConn = sourceTable.rfind('\\')
        srcConn = [:srcConn+1]
        try:
            srcEdit = arcpy.da.Editor(src[0])
        except:
            SrcIsNotGDB = True
    if dest == 'ODBC':
        DstIsNotGDB = True
    else:
        destConn = destTable.rfind('\\')
        destConn = [:destConn+1]
        try:
            arcpy.env.workspace = destConn
            destEdit = arcpy.da.Editor(arcpy.env.workspace)
        except:
            DstIsNotGDB = True              
    
    #if table doesn't work with arcpy - operation parameters are 'SEL' for selecting, 'DEL' for truncating, and 'INS' for copying
    def tryODBC(operation,intable,inval = None):
        logging.info('Performing {0} on ODBC'.format(operation))
        ix = intable.rfind('\\')
        table = intable[ix+1:]
        ix = table.rfind('.')
        tname1 = table[:ix]
        tname2 = table[ix+1:]
        tname = "["+tname1+"].["+tname2+"]"
        SQL=''
        s='{0} is succesful'
        if operation == 'DEL':
            SQL = 'TRUNCATE TABLE '+tname+';'
            s=s.format('Delete')
        elif operation == 'SEL':
            SQL="SELECT * FROM "+tname+';'
        elif operation == 'INS' and inval is not None:            
            for v in inval:
                SQL += 'INSERT INTO '+tname+' VALUES'
                ValList = "("
                for i in v:
                    if isinstance(i,basestring):
                        i='\''+i+'\''
                    ValList+=i+','
                ValList = ValList[0:len(ValList)-1]+');'
                SQL += ValList+'\n'
            s=s.format('Copy')
            print SQL
        elif operation == 'INS' and (inval is None or len(inval) == 0):
            n = "No rows copied"
            print n
            logging.info(n)
            return 0
        try:                
            db = pyodbc.connect(connection_string)
        except Exception as c:
            print c
            #sendtext("There was an error in the odbc connection: "+c)
            logging.warning('[{0}] : ERROR IN ODBC CONNECTION'.format(strftime("%Y-%m-%d %H:%M:%S", localtime())))
        try:
            db.cursor().execute(SQL)
            if operation =='SEL':
                rows = cursor.fetchall()
            else:
                db.commit()
            db.cursor().close()
            db.close()
            if operation =='SEL' and rows is not None:
                return rows
            elif operation =='SEL' and rows is None:
                n = "No rows found"
                print n
                logging.info(n)
                return 0
        except Exception as o:
                print o
                #sendtext("There was an error in the operation: "+o)
                logging.warning('[{0}] : ERROR IN OPERATION'.format(strftime("%Y-%m-%d %H:%M:%S", localtime())))
        #print s
        #logging.info(s)

    dothis = 'Transferring from {0} to {1}'.format(sourceTable,destTable)
    print dothis
    logging.info(dothis)
    #See if there's any data in source table and end script if there is not
    rowsList = []
    fieldsList = []
    if SrcIsNotGDB is False:
        #Skip OBJECTIDS or OIDS
        fNames = [f.name for f in arcpy.ListFields(sourceTable)]
        for f in fNames:
            if f=="OBJECTID" or f=="ESRI_OID":
                continue
            fieldsList.append(f)
        print fieldsList
        with arcpy.da.SearchCursor(sourceTable, fieldsList) as getProjectDataCurs:
            for r in getProjectDataCurs:
                rowsList.append(r)
        if len(rowsList) == 0:
            return
    else:
        rowsList = tryODBC('SEL',sourceTable)
        if rowsList == 0:
            n = "No rows found"
            print n
            logging.info(n)
            return
    #Truncate data
    if config.get("Options", "Truncate") == "Y" or config.get("Options", "Truncate").capitalize() == "Yes":
        destConn = destTable
        if DstIsNotGDB is False:
            print arcpy.env.workspace
            try:
                destEdit.startEditing(False,False)        
                destEdit.startOperation()
                with arcpy.da.UpdateCursor(destTable) as emptySourceCurs:
                    for r in emptySourceCurs:
                    emptySourceCurs.deleteRow(r)
                destEdit.stopOperation()
                destEdit.stopEditing()
            except:
                DstIsNotGDB = True
                tryODBC('DEL',destTable)
        else:
            tryODBC('DEL',destTable)
        s='Delete is succesful'
        print s
        logging.info(s)
    #Copy data
    if DstIsNotGDB is False:    
        try:
            destEdit.startEditing(False,False)
            destEdit.startOperation()
            with arcpy.da.InsertCursor(destTable, ['*']) as setDestRowCurs:               
                for r in rowsList:
                    setDestRowCurs.insertRow(r)
                    logging.info('[{0}] : INSERT {1}'.format(strftime("%Y-%m-%d %H:%M:%S", localtime()), r))
            destEdit.stopOperation()
            destEdit.stopEditing()
        except:
            DstIsNotGDB = True
            tryODBC('INS',destTable,rowsList)
    else:         
        tryODBC('INS',destTable,rowsList)
    s='Copy is succesful'
    print s
    logging.info(s)

def main(argv=None):   
    try:
        dir=config.get("Options","Direction")
        if dir == "1to2":
            transferData(Table1, Table2)
        elif dir == "2to1":
            transferData(Table2, Table1)   
        print "Done"
        logging.info("Done")
    except Exception as e:
       print e
       print traceback.format_exc()
       return

# Script start
if __name__ == "__main__":
    main()
