'use strict';

const {TableClient: AzureTableClient} = require('@azure/data-tables');

const AZURITE_TABLE_CONN = 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;';

module.exports = CatboxTableClient;

function CatboxTableClient(connection, partition, credential, options = {}) {
  if (options.client) {
    if (!(options.client instanceof AzureTableClient)) throw new TypeError('client must be an instance of azure TableClient');
    this.client = options.client;
  } else {
    if (!connection) {
      connection = AZURITE_TABLE_CONN;
    }

    const url = validateUrl(connection);
    if (!url) {
      this.client = AzureTableClient.fromConnectionString(connection, partition, options);
    } else {
      this.client = new AzureTableClient(connection, partition, credential, options);
    }
  }
  this.ready = false;
  this.connecting = false;
}

const proto = CatboxTableClient.prototype;

proto.get = async function get(partitionKey, id) {
  try {
    const record = await this.client.getEntity(partitionKey, id);
    try {
      var item = JSON.parse(record.item);
    } catch (e) {
      throw new Error(`Bad value content in segment "${partitionKey}" id "${id}"`);
    }
    return {
      item,
      stored: new Date(record.Timestamp).getTime(),
      ttl: record.ttl
    };
  } catch (err) {
    if (err.statusCode === 404) return null;
    throw err;
  }
};

proto.set = function set(partitionKey, rowKey, item, ttl, gc) {
  return this.client.upsertEntity({
    partitionKey,
    rowKey,
    ...(item ? {item: JSON.stringify(item), gc: !!gc} : undefined),
    ...(!isNaN(ttl) ? {
      ttl: Number(ttl),
      ttl_int: Date.now() + Number(ttl),
    } : undefined),
  }, 'Merge');
};

proto.connect = async function connect() {
  if (this.ready) return this.ready;
  if (this.connecting) return this.connecting;
  this.connecting = this.client.createTable();
  await this.connecting;
  this.ready = true;
  return this.connecting;
}

proto.disconnect = function disconnect() {
  this.connecting = null;
  this.ready = false;
}

proto.deleteTable = function deleteTable() {
  return this.client.deleteTable();
}

proto.evict = async function evict() {
  const resp = await this.getExpired();
  return await this.batchDelete(resp);
}

proto.drop = async function drop(partitionKey, rowKey) {
  try {
    const resp = await this.client.deleteEntity(partitionKey, rowKey);
    return resp;
  } catch (err) {
    if (err.statusCode === 404) return null;
    throw err;
  }
}

proto.getExpired = async function getExpired() {
  const query = [
    "gc eq true",
    "ttl_int lt " + Date.now(),
  ];

  const iterator = this.client.listEntities({
    queryOptions: {
      filter: query.join(" and "),
      select: ['partitionKey', 'rowKey'],
    },
  });

  const resp = await iterator.byPage({ maxPageSize: 100 });

  for await (const entities of resp) {
    return entities;
  }
};

proto.isReady = function isReady() {
  return this.ready;
}

proto.batchDelete = async function batchDelete(entries) {
  if (!entries.length) {
    return 0;
  }

  const batches = Object.values(createDeleteBatches(entries));

  // For some reason parallel delete batches across partitionKey doesn't work - 400 ResourceNotFound
  while (batches.length) {
    const batch = batches.shift();
    await this.client.submitTransaction(batch);
  }

  return entries.length;
};

function createDeleteBatches(entities) {
  return entities.reduce((result, entity) => {
    const {partitionKey, rowKey} = entity;
    let batch = result[partitionKey];
    if (!batch) {
      batch = result[partitionKey] = [];
    }
    batch.push(['delete', {partitionKey, rowKey}])
    return result;
  }, {});
}

function validateUrl(connStr) {
  try {
    const url = new URL(connStr);
    return url;
  } catch (err) {
    return false;
  }
}
