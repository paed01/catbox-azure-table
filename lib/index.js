/*eslint no-shadow:0 */
'use strict';

var Hoek = require('hoek');
var AzureStorage = require('azure-storage');
var Gc = require('./gc');

// Declare internals

var internals = {};

internals.defaults = {
    connection : 'UseDevelopmentStorage=true;DevelopmentStorageProxyUri=http://127.0.0.1;'
};

exports = module.exports = internals.Connection = function (options) {
    Hoek.assert(this.constructor === internals.Connection, 'Azure table storage cache client must be instantiated using new');
    Hoek.assert(typeof options === 'object', 'Must provide configuation to the Azure table storage cache client.');
    Hoek.assert(typeof options.partition === 'string', 'Must specify a partition (Azure storage table name) to use.');
    Hoek.assert(typeof options.ttl_interval === 'number' || options.ttl_interval === false, 'Must provide a ttl_interval or explicitly disable it');

    this.settings = Hoek.applyToDefaults(internals.defaults, options);
    this.client = null;

    return this;
};

internals.Connection.prototype.start = function (callback) {
    var self = this;

    if (self.client) {
        return callback();
    }

    self.client = AzureStorage.createTableService(self.settings.connection);
    AzureStorage.Validate.tableNameIsValid(self.settings.partition, function (terr) {
        if (terr) {
            return callback(terr);
        }

        self.tableName = self.settings.partition;

        self.client.createTableIfNotExists(self.tableName, function (err) {
            self.azuretablegc();
            callback(err);
        });
    });
};

internals.Connection.prototype.stop = function () {
    if (this.client) {
        if (this._gcfunc) {
            this._gcfunc.stop();
        }
        this.client = null;
    }
};

internals.Connection.prototype.isReady = function () {
    return this.client === null ? false : true;
};

internals.Connection.prototype.validateSegmentName = function (name) {
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
};

internals.Connection.prototype.get = function (key, callback) {
    var self = this;
    if (!self.client) {
        return callback(new Error('Connection not started'));
    }

    self.client.retrieveEntity(self.settings.partition, key.segment, key.id, function (err, record) {
        if (err && err.statusCode === 404) {
            return callback(null, null);
        } else if (err) {
            return callback(err);
        }

        var value = null;
        try {
            value = JSON.parse(record.item._);
        } catch (e) {
            return callback(new Error('Bad value content'));
        }

        var envelope = {
            item : value,
            stored : record.Timestamp._.getTime(),
            ttl : record.ttl._
        };

        callback(null, envelope);
    });
};

internals.Connection.prototype.generateRow = function (key, value, ttl, gc, callback) {
    var stringifiedValue = null;
    try {
        stringifiedValue = JSON.stringify(value);
    } catch (err) {
        return callback(err);
    }

    var entGen = AzureStorage.TableUtilities.entityGenerator;
    var ttl_int = Date.now() + ttl;
    var insertData = {
        PartitionKey : entGen.String(key.segment),
        RowKey : entGen.String(key.id),
        item : entGen.String(stringifiedValue),
        ttl : entGen.Int32(ttl),
        gc : entGen.Boolean(!!gc),
        ttl_int : entGen.Int64(ttl_int)
    };

    return callback(null, insertData);
};

internals.Connection.prototype.set = function (key, value, ttl, callback) {
    var self = this;
    if (!self.client) {
        return callback(new Error('Connection not started'));
    }

    self.generateRow(key, value, ttl, self._gcfunc, function (err, insertData) {
        if (err) {
            return callback(err);
        }

        self.client.insertOrMergeEntity(self.tableName, insertData, null, function (err) {
            callback(err);
        });
    });
};

internals.Connection.prototype.drop = function (key, callback) {
    var self = this;

    if (!self.client) {
        return callback(new Error('Connection not started'));
    }

    var entGen = AzureStorage.TableUtilities.entityGenerator;
    self.client.deleteEntity(self.tableName, {
        PartitionKey : entGen.String(key.segment),
        RowKey : entGen.String(key.id)
    }, function (err) {
        if (err) {
            if (err.statusCode === 404) {
                return callback(null);
            }

            return callback(err);
        }
        callback();
    });
};

internals.Connection.prototype.azuretablegc = function () {
    if (this.settings.ttl_interval === false) {
        this._gcfunc = null;
    } else {
        this._gcfunc = new Gc(this.settings);
        this._gcfunc.start();
    }
    return;
};

exports.Gc = Gc;
