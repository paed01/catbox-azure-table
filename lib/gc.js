'use strict';

const Hoek = require('hoek');
const AzureStorage = require('azure-storage');
const util = require('util');
const EventEmitter = require('events').EventEmitter;
const async = require('async');

let internals = {};

module.exports = internals.Gc = function(options) {
  Hoek.assert(this.constructor === internals.Gc, 'Azure table storage cache client must be instantiated using new');
  Hoek.assert(typeof options === 'object', 'Must provide configuation to the garbage collect module.');
  Hoek.assert(typeof options.connection === 'string', 'Must specify a connection (Azure storage table connection string) to use.');
  Hoek.assert(typeof options.partition === 'string', 'Must specify a partition (Azure storage table name) to use.');
  Hoek.assert(typeof options.ttl_interval === 'number', 'Must provide ttl_interval, interval in milliseconds to collect items');

  this.tableName = options.partition;
  this.settings = options;
  this.client = AzureStorage.createTableService(options.connection);
  this._timer = null;

  return this;
};

util.inherits(internals.Gc, EventEmitter);

function createBatches(entries) {
  let batches = {};

  entries.forEach((e) => {
    let partitionKey = e.PartitionKey._;
    let batch = batches[partitionKey];

    if (!batch) {
      batch = batches[partitionKey] = new AzureStorage.TableBatch();
    }

    try {
      batch.deleteEntity(e);
    } catch (err) {
      batch.error = err;
    }
  });

  return batches;
}

internals.Gc._createDeleteBatches = createBatches;

internals.Gc.prototype._get = function(callback) {
  const now = Date.now();
  const query = new AzureStorage.TableQuery()
    .top(100)
    .where('gc == ?bool?', true)
    .and('ttl_int < ?int64?', now);

  this.client.queryEntities(this.tableName, query, null, callback);
};

internals.Gc.prototype._delete = function(entries, callback) {
  const self = this;

  let batches = createBatches(entries);
  async.each(Object.keys(batches), (b, cb) => {
    const batch = batches[b];
    if (batch.error) return cb();
    self.client.executeBatch(self.tableName, batch, cb);
  }, (err) => {
    if (err) {
      self.emit('delete-error', err);
    }
    callback();
  });
};

internals.Gc.prototype.collect = function(callback) {
  const self = this;
  function innerCallback(err) {
    self.emit('collected', err);
    if (typeof callback === 'function') {
      callback(err);
    }
  }

  self._get((err, result) => {
    if (err || result.entries.length === 0) {
      return innerCallback(err);
    }

    return self._delete(result.entries, innerCallback);
  });
};

internals.Gc.prototype._start = function(callback) {
  if (this._timer) {
    this._timer = null;
  }

  this._timer = setTimeout(this.collect.bind(this, this._start.bind(this)), this.settings.ttl_interval);

  if (typeof callback === 'function') {
    return callback(null, this._timer);
  }
};

internals.Gc.prototype.start = function(callback) {
  if (this._timer) {
    clearTimeout(this._timer);
  }

  this._start(callback);
};

internals.Gc.prototype.stop = function() {
  if (this._timer) {
    this._timer.unref();
    clearTimeout(this._timer);
    this._timer = null;
  }
};
