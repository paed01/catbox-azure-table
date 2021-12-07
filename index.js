'use strict';

const TableClient = require('./lib/TableClient');
const Gc = require('./lib/gc');

module.exports = CatboxAzureTable;

function CatboxAzureTable(options) {
  const { connection, partition, ttl_interval, credential, ...opts } = options;

  if (ttl_interval !== false) {
    if (typeof ttl_interval !== 'number') throw new TypeError('Must provide a ttl_interval or explicitly disable it');
    if (ttl_interval < 1) throw new TypeError('ttl_interval must be a positive integer');
  }

  const client = this.client = new TableClient(connection, partition, credential, opts);
  if (ttl_interval) this.gc = new Gc(client, ttl_interval);
}

const proto = CatboxAzureTable.prototype;

proto.get = function get({ segment, id }) {
  return this.client.get(segment, id);
}

proto.set = function set({ segment, id }, value, ttl) {
  return this.client.set(segment, id, value, ttl, !!this.gc);
}

proto.drop = function drop({ segment, id }) {
  return this.client.drop(segment, id);
}

proto.start = async function start() {
  if (this.client.isReady()) return this.client;
  await this.client.connect();
  if (this.gc) this.gc.start();
}

proto.stop = function stop() {
  if (this.gc) this.gc.stop();
  this.client.disconnect();
}

proto.getClient = function getClient() {
  return this.client;
}

proto.isReady = function isReady() {
  return this.client.isReady();
}

proto.validateSegmentName = function validateSegmentName(name) {
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

module.exports.Gc = Gc;
