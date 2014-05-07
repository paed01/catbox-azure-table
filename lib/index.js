var Hoek = require('hoek');
var Azure = require('azure');

// Declare internals

var internals = {};

internals.defaults = {
    connection : 'UseDevelopmentStorage=true;DevelopmentStorageProxyUri=http://127.0.0.1;'
};

exports = module.exports = internals.Connection = function (options) {
    Hoek.assert(this.constructor === internals.Connection, 'Azure table storage cache client must be instantiated using new');
    Hoek.assert(typeof options === 'object', 'Must provide configuation to the Azure table storage cache client.');
    Hoek.assert(typeof options.partition === 'string', 'Must specify a partition (Azure storage table name) to use.');

    this.settings = Hoek.applyToDefaults(internals.defaults, options);
    this.client = null;
    
    return this;
};

internals.Connection.prototype.start = function (callback) {
    var _self = this;
    
    if (_self.client) {
        return callback();
    }
    
    _self.client = new Azure.createTableService(_self.settings.connection);
    _self.tableName = _self.settings.partition;
    
    _self.client.createTableIfNotExists( _self.tableName, function (err) {
        callback(err);
    });
};

internals.Connection.prototype.stop = function () {
    if (this.client) {
        this.client = null;
    }
};

internals.Connection.prototype.isReady = function () {
    return this.client == null ? false : true;
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
    var _self = this;
    if (!_self.client) {
        return callback(new Error('Connection not started'));
    }

    _self.client.queryEntity(_self.settings.partition, key.segment, key.id, function (err, record) {
        if (err) {
            if (err.statusCode === 404) {
                return callback(null);
            }

            return callback(err);
        }
        if (!record) {
            return callback(null, null);
        }

        var value = null;
        try {
            value = JSON.parse(record.item);
        } catch (err) {
            return callback(new Error('Bad value content'));
        }

        var envelope = {
            item : value,
            stored : record.Timestamp,
            ttl : record.ttl
        };

        callback(null, envelope);
    });
};

internals.Connection.prototype.set = function (key, value, ttl, callback) {
    var _self = this;
    if (!_self.client) {
        return callback(new Error('Connection not started'));
    }

    var stringifiedValue = null;
    try {
        stringifiedValue = JSON.stringify(value);
    } catch (err) {
        return callback(err);
    }

    var insertData = {
        PartitionKey : key.segment,
        RowKey : key.id,
        item : stringifiedValue,
        ttl : ttl
    };

    _self.client.insertOrMergeEntity(_self.tableName, insertData, function (err) {
        callback(err);
    });
};

internals.Connection.prototype.drop = function (key, callback) {
    var _self = this;

    if (!_self.client) {
        return callback(new Error('Connection not started'));
    }

    _self.client.deleteEntity(_self.tableName, {
        PartitionKey : key.segment,
        RowKey : key.id
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
