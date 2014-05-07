/*jshint expr:true es5:true */

var Catbox = require('catbox');

var Lab = require('lab');
var AzureTable = require('..');

var expect = Lab.expect;
var before = Lab.before;
// var beforeEach = Lab.beforeEach;
// var after = Lab.after;
var describe = Lab.experiment;
var it = Lab.test;

var options = {
    partition : 'unittestcache'
};

var antiOptions = {
    partition : 'unittestcache',
    connection : 'UseDevelopmentStorage=true;DevelopmentStorageProxyUri=http://192.168.0.1;'
};

describe('AzureTable', function () {
    it('throws an error if not created with new', function (done) {
        var fn = function () {
            var client = AzureTable();
        };

        expect(fn).to.throw(Error);
        done();
    });

    describe('#start', function () {
        it('returns no error', function (done) {
            var client = new Catbox.Client(AzureTable, options);
            client.start(function (err) {
                expect(err).to.not.exist;
                done();
            });
        });

        it('returns no error if called twice', function (done) {
            var client = new Catbox.Client(AzureTable, options);
            client.start(function (err) {
                expect(err).to.not.exist;
                client.start(done);
            });
        });

        it('returns error if partition (tableName) does not match naming convention', function (done) {
            var client = new Catbox.Client(AzureTable, {
                    connection : options.connection,
                    partition : 'cache-me'
                });
            client.start(function (err) {
                expect(err).to.exist;
                done();
            });
        });

        it('throws error if connection string is wrongly formatted', function (done) {
            var client = new Catbox.Client(AzureTable, {
                    connection : 'somewhere',
                    partition : options.partition
                });
            var fn = function () {
                client.start();
            };

            expect(fn).to.throw(Error);
            done();
        });
        
        it('throws error if azure storage doesn\'t exist', function (done) {
            var client = new Catbox.Client(AzureTable, antiOptions);
            client.start(function(err) {
                expect(err).to.exist;
                done();
            });
        });
    });

    describe('#stop', function () {
        it('returns no error', function (done) {
            var client = new Catbox.Client(AzureTable, options);
            client.start(function (err) {
                var fn = function () {
                    client.stop();
                };
                expect(fn).to.not.throw(Error);
                done();
            });

        });

        it('returns no error when client isn\'t started', function (done) {
            var client = new Catbox.Client(AzureTable, options);
            var fn = function () {
                client.stop();
            };

            expect(fn).to.not.throw(Error);
            done();
        });
    });

    describe('#isReady', function () {
        it('returns false if not started', function (done) {
            var client = new Catbox.Client(AzureTable, options);
            expect(client.isReady()).to.not.be.ok;
            done();
        });

        it('returns true if started', function (done) {
            var client = new Catbox.Client(AzureTable, options);
            client.start(function (err) {
                expect(client.isReady()).to.be.ok;
                done();
            });
        });
    });

    describe('#validateSegmentName', function () {
        it('returns null if validated', function (done) {
            var client = new Catbox.Client(AzureTable, options);
            expect(client.validateSegmentName('table')).to.be.null;
            done();
        });

        it('returns Error if empty string', function (done) {
            var client = new Catbox.Client(AzureTable, options);
            expect(client.validateSegmentName('')).to.be.instanceOf(Error);
            done();
        });

        it('returns Error if nothing passed', function (done) {
            var client = new Catbox.Client(AzureTable, options);
            expect(client.validateSegmentName()).to.be.instanceOf(Error);
            done();
        });

        it('returns Error if null', function (done) {
            var client = new Catbox.Client(AzureTable, options);
            expect(client.validateSegmentName(null)).to.be.instanceOf(Error);
            done();
        });

        it('returns Error if \\0', function (done) {
            var client = new Catbox.Client(AzureTable, options);
            expect(client.validateSegmentName('\0')).to.be.instanceOf(Error);
            done();
        });

    });

    describe('#set', function () {
        var client = new Catbox.Client(AzureTable, options);

        before(function (done) {
            client.start(function (err) {
                expect(err).to.not.exist;
                done();
            });
        });

        it('without started client returns error in callback', function (done) {
            var rawclient = new AzureTable(options);
            var d = {
                cache : true
            };
            var key = {
                id : 'item 2',
                segment : 'unittest'
            };
            rawclient.set(key, d, 10000, function (err) {
                expect(err).to.exist;
                done();
            });
        });

        it('puts object in cache', function (done) {
            var d = {
                cache : true
            };
            client.set({
                id : 'item 1',
                segment : 'unittest'
            }, d, 10000, function (err) {
                expect(err).to.not.exist;
                done();
            });
        });

        it('replaces object in cache', function (done) {
            var d = {
                cache : true
            };
            var key = {
                id : 'item 1 update',
                segment : 'unittest'
            };

            client.set(key, d, 10000, function (err) {
                expect(err).to.not.exist;

                var d2 = {
                    update : true
                };
                client.set(key, d2, 10000, function (err) {
                    client.get(key, function (err, data) {
                        expect(err).to.not.exist;

                        expect(data).to.exist;
                        expect(data.item).to.exist;
                        expect(data.item).to.be.an('object');
                        expect(data.item.cache).to.not.exist;
                        expect(data.item.update).to.equal(true);

                        done();
                    });
                });

            });
        });

        it('returns error in callback if circular json', function (done) {
            var d = {};
            d.circular = d;
            client.set({
                id : 'item 1',
                segment : 'unittest'
            }, d, 10000, function (err) {
                expect(err).to.exist;
                done();
            });
        });

        it('with started client returns error in callback', function (done) {
            var rawclient = new AzureTable(options);
            var d = {
                cache : true
            };
            var key = {
                id : 'item 2',
                segment : 'unittest'
            };
            rawclient.set(key, d, 10000, function (err) {
                expect(err).to.exist;
                done();
            });
        });
    });

    describe('#get', function () {
        var client = new Catbox.Client(AzureTable, options);
        before(function (done) {
            client.start(function (err) {
                expect(err).to.not.exist;
                done();
            });
        });

        it('without started client returns error in callback', function (done) {
            var rawclient = new AzureTable(options);
            var key = {
                id : 'item 2',
                segment : 'unittest'
            };
            rawclient.get(key, function (err) {
                expect(err).to.exist;
                done();
            });
        });

        it('fetches object from cache', function (done) {
            var key = {
                id : 'item 2',
                segment : 'unittest'
            };
            var d = {
                cache : true
            };
            client.set(key, d, 10000, function (err) {
                expect(err).to.not.exist;

                client.get(key, function (err) {
                    expect(err).to.not.exist;
                    done();
                });
            });
        });

        it('fetches object with same data', function (done) {
            var key = {
                id : 'item 3',
                segment : 'unittest'
            };
            var d = {
                cache : 'me',
                blue : false
            };
            client.set(key, d, 10000, function (err) {
                expect(err).to.not.exist;

                client.get(key, function (err, data) {
                    expect(err).to.not.exist;

                    expect(data).to.exist;
                    expect(data.item).to.exist;
                    expect(data.item).to.be.an('object');
                    expect(data.item.cache).to.equal(d.cache);
                    expect(data.item.blue).to.equal(false);

                    done();
                });
            });
        });

        it('with non-existing key id returns nothing', function (done) {
            var key = {
                id : 'no-item 1',
                segment : 'unittest'
            };
            client.get(key, function (err, data) {
                expect(err).to.not.exist;
                expect(data).to.not.exist;
                done();
            });
        });

        it('with non-existing key segment returns nothing', function (done) {
            var key = {
                id : 'no-item 1',
                segment : 'unittest-non-existing'
            };
            client.get(key, function (err, data) {
                expect(err).to.not.exist;
                expect(data).to.not.exist;
                done();
            });
        });

        it('with non-json data in table returns error in callback', function (done) {
            var key = {
                id : 'Wrongly formatted 1',
                segment : 'unittest'
            };
            var insertData = {
                PartitionKey : key.segment,
                RowKey : key.id,
                item : '[Object weee]',
                ttl : 10
            };

            client.connection.client.insertOrMergeEntity(client.connection.tableName, insertData, function (err) {
                expect(err).to.not.exist;

                client.get(key, function (err, data) {
                    expect(err).to.exist;
                    expect(err.message).to.equal('Bad value content');
                    done();
                });

            });
        });

    });

    describe('#drop', function () {
        var client = new Catbox.Client(AzureTable, options);
        before(function (done) {
            client.start(function (err) {
                expect(err).to.not.exist;
                done();
            });
        });

        it('without started client returns error in callback', function (done) {
            var rawclient = new AzureTable(options);
            var key = {
                id : 'item 4',
                segment : 'unittest'
            };
            rawclient.drop(key, function (err) {
                expect(err).to.exist;
                done();
            });
        });

        it('drops object from cache', function (done) {
            var key = {
                id : 'item 4',
                segment : 'unittest'
            };
            var d = {
                cache : true
            };
            client.set(key, d, 10000, function (err) {
                expect(err).to.not.exist;

                client.drop(key, function (err) {
                    expect(err).to.not.exist;
                    client.get(key, function (err, data) {
                        expect(err).to.not.exist;
                        expect(data).to.not.exist;
                        done();
                    });
                });
            });
        });

        it('with non-existing key id returns nothing', function (done) {
            var key = {
                id : 'no-item 2',
                segment : 'unittest'
            };
            client.drop(key, function (err) {
                expect(err).to.not.exist;
                done();
            });
        });

        it('with non-existing segment returns nothing', function (done) {
            var key = {
                id : 'no-item 2',
                segment : 'unittest-non-existing'
            };
            client.get(key, function (err) {
                expect(err).to.not.exist;
                done();
            });
        });

    });
});
