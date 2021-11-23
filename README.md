catbox-azure-table
==================

[![Build status](https://ci.appveyor.com/api/projects/status/anrkvdxmentpi1e5/branch/default?svg=true)](https://ci.appveyor.com/project/paed01/catbox-azure-table/branch/default) [![Coverage Status](https://coveralls.io/repos/github/paed01/catbox-azure-table/badge.svg?branch=default)](https://coveralls.io/github/paed01/catbox-azure-table?branch=default)

Azure Table Storage adapter for [catbox](https://github.com/hapijs/catbox)

| Version           | catbox version                                                   |
| ----------------- | ---------------------------------------------------------------- |
| 5.0.0             | @hapi/catbox@11 and azure-storage@2 defined as peer dependencies |
| 4.0.0             | [@hapi/catbox@11](https://www.npmjs.com/package/@hapi/catbox)    |
| 3.2.1             | [catbox@10](https://www.npmjs.com/package/catbox)                |

### Options

- `connection` - the [Azure Storage connection string](https://www.connectionstrings.com/windows-azure/). Defaults to `UseDevelopmentStorage=true;DevelopmentStorageProxyUri=http://127.0.0.1;`.
- `partition` - the partition will set the table name of your cache. Defaults to `catbox` when used from module `catbox`.
- `ttl_interval` -  the interval at which the [GC function](#garbage-collection) will run in milliseconds, pass `false` to bypass running the GC function.

### Notes

The default option for connection is set to use the [Azure Storage Emulator][1] (v5.10.0.0). This feature must be installed and started to run the tests.

When setting or getting cache-items `segment` translates to Azure Table Storage `partitionKey`.

### Garbage Collection

Since Azure Table Storage have no built-in ttl function this functionality will delete expired keys if `ttl_interval` is specified.

If `ttl_interval` is set to `false` the cached entries stored in the table will not be collected even if another `catbox-azure-table` client points to the same table.

Due to a limitation in Azure Table Storage only 100 entries can be deleted at one time, per `PartitionKey` (`segment`). This can cause a problem if there is more than 100 entries created within the `ttl_interval`. But probably the GC will catch up eventually.

[1]: https://azure.microsoft.com/en-us/documentation/articles/storage-use-emulator/
