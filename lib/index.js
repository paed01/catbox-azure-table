'use strict';

const TableClient = require('./TableClient');
const Gc = require('./gc');

module.exports = function Connection(options) {
  if (typeof options !== 'object') throw new Error('Must provide configuration to the Azure table storage cache client.');

  const {connection, partition, ttl_interval} = options;

  if (typeof partition !== 'string') throw new TypeError('Must specify a partition (Azure storage table name) to use.');
  if (typeof ttl_interval !== 'number' && ttl_interval !== false) throw new TypeError('Must provide a ttl_interval or explicitly disable it');

  const client = TableClient(connection, partition);
  let gc;
  if (ttl_interval) gc = Gc(client, ttl_interval);

  return {
    gc,
    drop,
    get,
    getClient,
    isReady,
    set,
    start,
    stop,
    validateSegmentName
  };

  async function get({segment, id}) {
    if (!isReady()) {
      throw new Error('Connection not started');
    }

    return await client.get(segment, id);
  }

  async function set({segment, id}, value, ttl) {
    if (!isReady()) {
      throw new Error('Connection not started');
    }

    return await client.set(segment, id, value, ttl, !!gc);
  }

  async function drop({segment, id}) {
    if (!isReady()) {
      throw new Error('Connection not started');
    }

    return await client.drop(segment, id);
  }

  async function start() {
    if (client.isReady()) return client;
    await client.connect();
    if (gc) gc.start();
  }

  function stop() {
    if (gc) gc.stop();
    client.disconnect();
  }

  function getClient() {
    return client;
  }

  function isReady() {
    const gcReady = gc ? gc.isReady() : true;
    return client.isReady() && gcReady;
  }

  function validateSegmentName(name) {
    // The forward slash (/) character
    // The backslash (\) character
    // The number sign (#) character
    // The question mark (?) character
    // Control characters from U+0000 to U+001F, including:
    // The horizontal tab (\t) character
    // The linefeed (\n) character
    // The carriage return (\r) character
    // Control characters from U+007F to U+009F

    // The above is validated by Azure module

    if (!name) {
      return new Error('Empty string');
    }

    if (name.indexOf('\0') !== -1) {
      return new Error('Includes null character');
    }

    return null;
  }
};

module.exports.Gc = Gc;
