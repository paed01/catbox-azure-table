catbox-azure-table
===========

Azure Storage table adapter for catbox

Install **catbox-azure-table**:
```
npm install catbox-azure-table
```

### Options

- `connection` - the Azure storage connection string. Defaults to `UseDevelopmentStorage=true;DevelopmentStorageProxyUri=http://127.0.0.1;`.
- `partition` - the partition will set the Azure Storage table name for your cache.


### Notes

The default option for connection is set to use the Windows Azure Emulator. This feature must be installed and started to runt the tests.