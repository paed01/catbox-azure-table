catbox-azure-table
===========

Azure Storage table adapter for [catbox](https://github.com/spumko/catbox)

Install **catbox-azure-table**:
```
npm install catbox-azure-table
```

### Options

- `connection` - the [Azure Storage connection string](https://www.connectionstrings.com/windows-azure/). Defaults to `UseDevelopmentStorage=true;DevelopmentStorageProxyUri=http://127.0.0.1;`.
- `partition` - the partition will set the Azure Storage table name of your cache. Defaults to `catbox`.


### Notes

The default option for connection is set to use the [Windows Azure Emulator](http://msdn.microsoft.com/en-us/library/azure/hh403989.aspx). This feature must be installed and started to run the tests.

When setting or getting cache-items `segment` translates to Azure Table `partitionKey`.