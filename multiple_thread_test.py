# -*- coding: utf-8 -*-
from pybase import *
from hbase.ttypes import *

from hashlib import md5
import progressbar
import threading
import time
import random

class ThreadSafeProgress(threading.Thread):
    def __init__(self, maxval):
        threading.Thread.__init__(self)
        self.widgets = ['Insert Values: ', progressbar.Percentage(), ' ', progressbar.Bar('>'), ' ', progressbar.ETA(), ' ', progressbar.FileTransferSpeed()]
        self.prog = progressbar.ProgressBar(widgets = self.widgets, maxval=maxval)
        self.count = 0
        self._local = threading.local()
        #note: thread should be created from the same thread, otherwise the id is not threadsafe
        self.xthreads_progress = 0
        self.__stop = threading.Event()
        self.lk = threading.Lock()
    

    def stop(self):
        self.__stop.set()

    def inc(self, num=1):
        self.lk.acquire()
        self.xthreads_progress+=num
        self.lk.release()

    def run(self):
        self.prog.start()
        while not self.__stop.isSet():
            time.sleep(1)
            self.lk.acquire()
            tillnow = self.xthreads_progress
            self.lk.release()
            self.prog.update(tillnow)

class fillHbase(threading.Thread):
    def __init__ (self,num_keys, tab, progress):
        threading.Thread.__init__(self)
        self.num_keys = num_keys
        self.tab = tab
        self.progress = progress

    def run(self):
        entropy = random.randint(1, 10000)
        h = md5()
        value = "\0"*1024*2
        for i in range(self.num_keys):
            h.update("%s-%d-%d" % (self.ident, i, entropy))
            row=h.digest()
            mods = {"foo:ciao":"%s" % i}
            self.tab.insert(row, mods)
            self.progress.inc()
            




NUMTHREAD=5
TABNAME = 'pycassahtable'
KEYSPERTHREAD=int(1e2)
client = connect_thread_local(['hdnn:9090'])
tab = HTable(client, TABNAME,[ColumnDescriptor(name='foo:'), ColumnDescriptor(name='foo2:')], createIfNotExist=True, overwrite=False)
print "TABELLA", tab

progress = ThreadSafeProgress(NUMTHREAD*KEYSPERTHREAD)
progress.start()
workertable = []
for i in range(NUMTHREAD):
    worker = fillHbase(KEYSPERTHREAD, tab, progress)
    worker.start()
    workertable.append(worker)

print "Waiting the end"
for wk in workertable:
    wk.join()

progress.stop()
progress.join()

print tab.getTableRegions()

#while 1:
    #try:
        
        #print "Main thread working..."
        #time.sleep(0.5)
        ##if i == 10:
            ##print "Adding 3 more worker threads..."
            ##main.createWorkers(3)
        ##i += 1
    #except KeyboardInterrupt:
        #print "Interrupted!"
        #break
    #except NoResultsPending:
        #print "All results collected."
        #break

