/*eslint camelcase:0 */

var Hoek = require('hoek');
var AzureStorage = require('azure-storage');
var util = require('util');
var EventEmitter = require('events').EventEmitter;
var async = require('async');

var internals = {};

exports = module.exports = internals.Gc = function (options) {
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

internals.Gc.prototype._get = function (callback) {
	var now = Date.now();
	var query = new AzureStorage.TableQuery()
		.top(100)
		.where('gc == ?bool?', true)
		.and('ttl_int < ?int64?', now);

	this.client.queryEntities(this.tableName, query, null, callback);
};

internals.Gc.prototype._createBatches = function (entries, callback) {
	var batches = {};

	async.each(entries, function (e, cb) {
		var partitionKey = e.PartitionKey._;
		var batch = batches[partitionKey];

		if (!batch) {
			batch = batches[partitionKey] = new AzureStorage.TableBatch();
		}

		var err;
		try {
			batch.deleteEntity(e);
		} catch (derr) {
			err = derr;
		}

		cb(err);
	}, function (err) {
		if (err) {
			return callback(err);
		}

		callback(null, batches);
	});
};

internals.Gc.prototype._delete = function (entries, callback) {
	var _self = this;

	_self._createBatches(entries, function (err, batches) {
		if (err) {
			return callback(err);
		}

		async.each(Object.keys(batches), function (b, cb) {
			var batch = batches[b];

			_self.client.executeBatch(_self.tableName, batch, function (err, result) {
				_self.emit('batch-result', err, result);
				cb();
			});
		}, callback);
	});

};

internals.Gc.prototype.collect = function (callback) {
	var _self = this;
	var innerCallback = function (err) {
		_self.emit('collected', err);
		if (typeof callback === 'function') {
			callback(err);
		}
	};

	_self._get(function (err, result) {
		if (err || result.entries.length === 0) {
			return innerCallback(err);
		}

		_self._delete(result.entries, innerCallback);
	});
};

internals.Gc.prototype._start = function(callback) {
    if (this._timer) {
        this._timer = null;
    }

    this._timer = setTimeout(this.collect.bind(this, this._start.bind(this)), this.settings.ttl_interval);

    if (typeof callback === 'function') {
        callback(null, this._timer);
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
