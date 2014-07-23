catbox-azure-table
===========

[![Dependency Status](https://david-dm.org/paed01/catbox-azure-table.png)](https://david-dm.org/paed01/catbox-azure-table)

Azure Storage table adapter for [catbox](https://github.com/spumko/catbox)

Install **catbox-azure-table**:
```
npm install catbox-azure-table
```

### Options

- `connection` - the [Azure Storage connection string](https://www.connectionstrings.com/windows-azure/). Defaults to `UseDevelopmentStorage=true;DevelopmentStorageProxyUri=http://127.0.0.1;`.
- `partition` - the partition will set the Azure Storage table name of your cache. Defaults to `catbox`.
- `ttl_interval` -  the interval at which the module [GC function](#garbage_collection) will run in milliseconds, pass `false` to bypass running the GC function.

### Notes

The default option for connection is set to use the [Windows Azure Emulator](http://msdn.microsoft.com/en-us/library/azure/hh403989.aspx). This feature must be installed and started to run the tests.

When setting or getting cache-items `segment` translates to Azure Table `partitionKey`.

### Garbage Collection

Since Azure Storage tables have no built-in ttl function this functionality will delete expired keys if `ttl_interval` is an integer.

If `ttl_interval` is set to `false` the cached entries stored in the table will not be collected even if another `catbox` client points to the same table.

Due to a limitation in Azure Storage only 100 entries can be deleted at one time, per `PartitionKey` (`segment`). This can cause a problem if there is more than 100 entries created within the `ttl_interval`. But probably the GC will catch up eventually.