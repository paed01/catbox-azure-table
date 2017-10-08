'use strict';

const {createTableService, TableBatch, TableQuery, TableUtilities, Validate} = require('azure-storage');
const {String: MakeString, Boolean: MakeBoolean, Int32: MakeInt32, Int64: MakeInt64} = TableUtilities.entityGenerator;

module.exports = function TableClient(connectionString, tableName) {
  let client, connection;

  Validate.tableNameIsValid(tableName);

  return {
    tableName,
    connectionString,
    connect,
    disconnect,
    deleteTable,
    evict,
    drop,
    get,
    getClient,
    getExpired,
    insert,
    isReady,
    set,
  };

  async function connect() {
    if (client) return client;

    const tableService = getTableService();
    return new Promise((resolve, reject) => {
      tableService.createTableIfNotExists(tableName, (err) => {
        if (err) return reject(err);
        client = tableService;
        return resolve(client);
      });
    });
  }

  function getTableService() {
    if (connection) return connection;
    connection = createTableService(connectionString);
    return connection;
  }

  function set(partitionKey, rowKey, item, ttl, gc) {
    const entity = createEntity(partitionKey, rowKey, item, ttl, gc);
    return insert(entity);
  }

  async function insert(entity) {
    const tableService = await connect();

    return new Promise((resolve, reject) => {
      tableService.insertOrMergeEntity(tableName, entity, null, (err, ...args) => {
        if (err) return reject(err);
        return resolve(...args);
      });
    });
  }

  async function drop(partitionKey, rowKey) {
    const tableService = await connect();
    const entity = createEntity(partitionKey, rowKey);

    return new Promise((resolve, reject) => {
      tableService.deleteEntity(tableName, entity, null, (err, ...args) => {
        if (err) {
          if (err.statusCode === 404) return resolve(null);
          return reject(err);
        }
        return resolve(...args);
      });
    });
  }

  async function get(partitionKey, id) {
    const tableService = await connect();

    return new Promise((resolve, reject) => {
      tableService.retrieveEntity(tableName, partitionKey, id, (err, record) => {
        if (err) {
          if (err.statusCode === 404) return resolve(null);
          return reject(err);
        }

        let value = null;
        try {
          value = JSON.parse(record.item._);
        } catch (e) {
          return reject(new Error(`Bad value content in segment "${partitionKey}" id "${id}"`));
        }

        const envelope = {
          item: value,
          stored: record.Timestamp._.getTime(),
          ttl: record.ttl._
        };

        resolve(envelope);
      });
    });
  }

  async function getExpired() {
    const tableService = await connect();

    const query = new TableQuery()
      .top(100)
      .where('gc == ?bool?', true)
      .and('ttl_int < ?int64?', Date.now());

    return new Promise((resolve, reject) => {
      tableService.queryEntities(tableName, query, null, (err, ...args) => {
        if (err) return reject(err);
        return resolve(...args);
      });
    });
  }

  async function evict() {
    const {entries} = await getExpired();
    return await batchDelete(entries);
  }

  function batchDelete(entries) {
    if (!entries) return;
    if (!entries.length) {
      return entries.length;
    }

    const batches = createDeleteBatches(entries);
    const deletes = Object.keys(batches).map((partitionKey) => {
      const tableBatch = batches[partitionKey];
      return batch(tableBatch);
    });

    return Promise.all(deletes).then(() => {
      return entries.length;
    });
  }

  async function batch(tableBatch) {
    return new Promise((resolve, reject) => {
      client.executeBatch(tableName, tableBatch, (err, ...args) => {
        if (err) return reject(err);
        return resolve(...args);
      });
    });
  }

  async function deleteTable() {
    const tableService = getTableService();

    return new Promise((resolve, reject) => {
      tableService.deleteTableIfExists(tableName, (err, ...args) => {
        if (err) return reject(err);
        client = null;
        return resolve(...args);
      });
    });
  }

  function disconnect() {
    client = null;
  }

  function getClient() {
    return client;
  }

  function isReady() {
    return !!client;
  }
};

function createEntity(partitionKey, rowKey, item, ttl, gc) {
  const entity = {
    PartitionKey: MakeString(partitionKey),
    RowKey: MakeString(rowKey)
  };

  if (item) {
    entity.item = MakeString(JSON.stringify(item));
    entity.gc = MakeBoolean(!!gc);
  }
  if (!isNaN(ttl)) {
    entity.ttl = MakeInt32(ttl);
    entity.ttl_int = MakeInt64(Date.now() + ttl);
  }

  return entity;
}

function createDeleteBatches(entries) {
  return entries.reduce((result, entry) => {
    const partitionKey = entry.PartitionKey._;
    let batch = result[partitionKey];
    if (!batch) {
      batch = result[partitionKey] = new TableBatch();
    }

    try {
      batch.deleteEntity(entry);
    } catch (err) {
      batch.error = err;
    }

    return result;
  }, {});
}
