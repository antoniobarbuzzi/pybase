#!/usr/bin/env python
# -*- coding: utf-8 -*-

from connection import *
from hbase.ttypes import *

__all__ = ['HTable']

#TODO: strip admin/user functions of HTable

class HTable(object):
    def __init__(self, client, tableName, columnFamiliesList=[], createIfNotExist=False, overwrite=False):
        self._client = client
        self._tableName = tableName
        self._columnFamiliesList = columnFamiliesList
        self._createIfNotExist = createIfNotExist
        self._overwrite = overwrite
        if createIfNotExist:
            try:
                self._client.createTable(tableName, columnFamiliesList)
            except AlreadyExists, tx:
                if not overwrite:
                    print "Thrift exception"
                    print '%s' % (tx.message)
                    print "Overwrite not setted"
                    #raise
                else:
                    self.dropTable(tableName)
                    self._client.createTable(tableName, columnFamiliesList)

    def dropTable(self, tabName):
        self._client.disableTable(self._tableName)
        self._client.deleteTable(self._tableName)

    def __str__(self):
        descr = "%s (" % self._tableName + ", ".join(["%s" % cf.name for cf in self._columnFamiliesList]) + ")"
        return descr

    def __repr__(self):
        #print "TABLES:", self._local.client.getTableNames()
        #print "DESCRIPTORS:", self._local.client.getColumnDescriptors(TABLE_NAME)
        return "%s", self._client.getColumnDescriptors(self._tableName)

    
    #def close(self):
        #self.client.transport.close()

    #def insert(self, row, column, value):
        #assert(column.find(':') != -1)
        #self._client.mutateRow(self._tableName, row, [Mutation(column=column, value=value)])

    def insert(self, row, mutations):
        """
        Apply a series of mutations (updates/deletes) to a row in a
        single transaction.  If an exception is thrown, then the
        transaction is aborted.  Default current timestamp is used, and
        all entries will have an identical timestamp.
        
        @param row row key
        @param mutations list of mutation commands, as a dict of column:value
            ex. mutations = {'person:name':'Antonio'}
        """
        mutations = [Mutation(column=k, value=v) for (k,v) in mutations.iteritems()]
        self._client.mutateRow(self._tableName, row, mutations)
    
    def insertTs(self, row, mutations, ts): # untested
        """
        Apply a series of mutations (updates/deletes) to a row in a
        single transaction.  If an exception is thrown, then the
        transaction is aborted. A timestamp value is required, and
        all entries will have an identical timestamp.
        
        @param row row key
        @param mutations list of mutation commands, as a dict of column:value
            ex. mutations = {'person:name':'Antonio'}
        @param ts timestamp
        """
        mutations = [Mutation(column=k, value=v) for (k,v) in mutations.iteritems()]
        self._client.mutateRowTs(self._tableName, row, mutations, ts)

    #def scanner.old(self, startRow="", columnlist=""):
        #scanner = self._client.scannerOpen(self._tableName, startRow, columnlist)
        #def next():
            #while True:
                #ret = self._client.scannerGet(scanner)
                #if not ret:
                    #break
                #yield ret
            #self._client.scannerClose(scanner)
        #return next()


    def scannerPrefix(self, startRowPrefix="", columnlist=""):
        raise NotImplemented()
        pass

    def scanner(self, startRow="", stopRow="", columnlist="", timestamp=None):
        '''
        Get a scanner on the current table starting at the specified row and
        ending at the last row in the table.  Return the specified columns.
        Only values with the specified timestamp are returned.
        @param startRow starting row in table to scan.  send "" (empty string) to
                start at the first row.
        @param stopRow row to stop scanning on.  This row is *not* included in the
                scanner's results
        @param columnList columns to scan. If column name is a column family, all
                columns of the specified column family are returned.  Its also possible
                to pass a regex in the column qualifier.
        @param timestamp timestamp
        
        @return scanner iterator
        '''
        if stopRow: #untested
            if timestamp: #untested
                scanner = self._client.scannerOpenWithStopTs(self._tableName, startRow, stopRow, columnlist, timestamp)
            else: #untested
                scanner = self._client.scannerOpenWithStop(self._tableName, startRow, stopRow, columnlist)
        elif timestamp: #untested
            scanner = self._client.scannerOpenTs(self._tableName, startRow, columnlist, timestamp)
        else: #tested
            scanner = self._client.scannerOpen(self._tableName, startRow, columnlist)

        def next(n=1):
            while True:
                ret = self._client.scannerGetList(scanner, n)
                if not ret:
                    break
                yield ret
            self._client.scannerClose(scanner)
        return next()

    def getTableRegions(self):
        return self._client.getTableRegions(self._tableName)


if __name__ == '__main__':
    from hashlib import md5
    import progressbar

    TABNAME = 'pycassahtable'
    KEYS=1000
    client = connect_thread_local(['hdnn:9090'])
    client.getTableNames()
    tab = HTable(client, TABNAME,[ColumnDescriptor(name='foo:'), ColumnDescriptor(name='foo2:')], overwrite=True) 
    print "TABELLA", tab
    
    h = md5()
    widgets = ['Insert Values: ', progressbar.Percentage(), ' ', progressbar.Bar('>'), ' ', progressbar.ETA(), ' ', progressbar.FileTransferSpeed()]
    prog = progressbar.ProgressBar(widgets = widgets, maxval=KEYS).start()
    for i in range(KEYS):
        h.update("%d" % i)
        row=h.digest()
        data = {"foo:ciao":"%s"%i}
        tab.insert(row, data)
        prog.update(i)

    it = tab.scanner('', '')
    for i in it:
        print i

    print tab.getTableRegions()
    client.close()

