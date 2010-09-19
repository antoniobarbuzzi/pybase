# -*- coding: utf-8 -*-
from pybase import *
from hbase.ttypes import *

if __name__ == '__main__':
    from hashlib import md5
    import progressbar

    TABNAME = 'pycassahtable'
    KEYS=int(1e2)
    client = connect_thread_local(['hdnn:9090'])
    client.getTableNames()
    tab = HTable(client, TABNAME,[ColumnDescriptor(name='foo:'), ColumnDescriptor(name='foo2:')], createIfNotExist=True, overwrite=True)
    print "TABELLA", tab

    h = md5()
    widgets = ['Insert Values: ', progressbar.Percentage(), ' ', progressbar.Bar('>'), ' ', progressbar.ETA(), ' ', progressbar.FileTransferSpeed()]
    prog = progressbar.ProgressBar(widgets = widgets, maxval=KEYS).start()
    for i in range(KEYS):
        h.update("%d" % i)
        row=h.digest()
        value = "%s" % i
        mods = {"foo:ciao":value, "foo:ciao2":value}
        tab.insert(row, mods)
        #tab.insert(row=row, column="foo:ciao", value="%s" % i)
        prog.update(i)

    print "Testing Scanner"
    print "Scan all rows"
    it = tab.scanner()
    for i in it:
        print i
    print

    print "Scan starting from {0}".format(repr('\xd9dd\xc96F\x9cq\xe2b\xf9\xdf\xd3\x88J\xfe'))
    it = tab.scanner(startRow='\xd9dd\xc96F\x9cq\xe2b\xf9\xdf\xd3\x88J\xfe')
    for i in it:
        print i
    print

    print "Scan starting from {0} to {1}".format(repr('\xd9dd\xc96F\x9cq\xe2b\xf9\xdf\xd3\x88J\xfe'), repr('\xe5Q\xd2\xc1\xf4 6\xac\xb4\\\xadt\x19d\xe74'))
    it = tab.scanner(startRow='\xd9dd\xc96F\x9cq\xe2b\xf9\xdf\xd3\x88J\xfe', stopRow='\xe5Q\xd2\xc1\xf4 6\xac\xb4\\\xadt\x19d\xe74')
    for i in it:
        print i
    print

    print "Scan starting from {0} to {1} column='foo:ciao'".format(repr('\xd9dd\xc96F\x9cq\xe2b\xf9\xdf\xd3\x88J\xfe'), repr('\xe5Q\xd2\xc1\xf4 6\xac\xb4\\\xadt\x19d\xe74'))
    it = tab.scanner(startRow='\xd9dd\xc96F\x9cq\xe2b\xf9\xdf\xd3\x88J\xfe', stopRow='\xe5Q\xd2\xc1\xf4 6\xac\xb4\\\xadt\x19d\xe74', columnlist=['foo:ciao'])
    for i in it:
        print i
    print

    print "TABLE REGIONS:"
    print tab.getTableRegions()

