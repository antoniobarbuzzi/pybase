pybase
=======

pybase is a python library for HBase, based on Pycassa (http://github.com/pycassa/pycassa), a python library for Cassandra.

It inherits some pycassa features, therefore:

1. Simplified thrift interface
2. Thread safe

I do not need it anymore, therefore the development is very, very slow.

Documentation
-------------

Not very well documented, it was just a private project. Read single_thread_test.py e multiple_thread_test.py to understand how to use it.


Requirements
------------

    thrift: http://incubator.apache.org/thrift/
    HBase: http://hbase.apache.org

To install thrift's python bindings:

    easy_install thrift

pybase comes with the HBase python files for convenience, but you can replace them with your own.

Installation
------------

Just copy the directory in your program, or in your $PYTHONPATH..

Connecting
----------

connection.py is just a version of the same file of pycassa, with minor modifications. So, you can find even docstrings referring to Cassandra, but do not worry about.


Basic Usage
----------

Start HBase Thrift server:
    
    [hbase-root]/bin/hbase thrift start

Connect to it:
    
    client = connect_thread_local(['hdnn:9090'])

List tables:
    
    client.getTableNames()

Access(/create) a table:
    
    tab = HTable(client, TABNAME, [ColumnDescriptor(name='foo:'), ColumnDescriptor(name='foo2:')], createIfNotExist=True, overwrite=False)

Choose the row where to put some data:
    
    row = "newkey"

Create a dictionary with the data to insert (or to modify) for your key:
    
    changes = {"foo:name":"Antonio", "foo:surname":"Barbuzzi"}
    
Insert new data in the table:
    
    tab.insert(row, changes)

Access to all data in the table, using a scanner on it:
    
    it = tab.scanner()
    for i in it:
        print i

Note that scanner supports more parameters, see its documentation or the examples.

Print all the Regions for a table:
    
    print tab.getTableRegions()
    
Other features, you should just look to the code

Extending pybase and accessing Thrift API
----------


Connection overrides __getattr__, so if want to call a function defined in the Thrift HBase API using Connection, a new function is dinamically created that access the Thrift API

Note that HTable saves an instance of the Connection (ThreadLocalConnection or SingleConnection), therefore, for example, when Htable.getTableRegions() calls ThreadLocalConnection.getTableRegions (or SingleLocalConnection), the new function is created at runtime (there isn't any getTableRegions in Connection).

    class HTable:
        # cut
        def getTableRegions(self):
            return self._client.getTableRegions(self._tableName)
