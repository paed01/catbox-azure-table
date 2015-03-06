/*jshint expr:true, es5:true, camelcase:false */
/*eslint camelcase:0, no-unused-vars:0, handle-callback-err:0 */
'use strict';

var AzureStorage = require('azure-storage');
var async = require('async');
var Hoek = require('hoek');
var util = require('util');

var Lab = require('lab');
var AzureTable = require('..');
var Gc = AzureTable.Gc;

var expect = require('code').expect;

var lab = exports.lab = Lab.script();

var options = {
    connection : process.env.AZURE_TABLE_CONN,
    partition : 'unittestcachegc',
    ttl_interval : 0
};

lab.experiment('AzureTable GC', function () {
    var atableClient = new AzureTable(options);
    var settings = Hoek.clone(atableClient.settings);
    settings.ttl_interval = 5000;

    lab.before(function (done) {
        atableClient.start(done);
    });

    lab.experiment('#ctor', function () {
        lab.test('throws an error if not created with new', function (done) {
            var fn = function () {
                Gc();
            };

            expect(fn).to.throw(Error);
            done();
        });
    });

    lab.experiment('#start', function () {
        lab.test('starts timer', function (done) {
            var gc = new Gc(settings);
            gc.start(function (err, timer) {
                expect(timer).to.be.an.object();
                gc.stop();
                done();
            });
        });

        lab.test('restarts timer if already started', function (done) {
            var gc = new Gc(settings);
            gc.start(function (err, timer1) {
                gc.start(function (err, timer2) {
                    expect(timer1).to.not.equal(timer2);
                    gc.stop();
                    done();
                });
            });
        });
    });

    lab.experiment('#stop', function () {
        lab.test('stops timer', function (done) {
            var gc = new Gc(settings);
            gc.start();
            gc.stop();
            expect(gc._timer).to.equal(null);
            done();
        });

        lab.test('does nothing if not started', function (done) {
            var gc = new Gc(settings);
            gc.stop();
            expect(gc._timer).to.equal(null);
            done();
        });
    });

    lab.experiment('#collect', function () {
        lab.test('deletes expired items', function (done) {
            var gcExp = new Gc(settings);
            var segment = 'ttltest1';
            var set = function (id, callback) {
                atableClient.set({
                    id : id,
                    segment : segment
                }, {
                    cacheme : true
                }, 50, callback);
            };

            var itemIds = ['1', '2', '3'];

            async.each(itemIds, function (id, cb) {
                set(id, cb);
            }, function (err) {
                expect(err).to.not.exist();

                console.log('listening on event');
                gcExp.once('collected', function (err) {
                    expect(err).to.not.exist();

                    atableClient.get({
                        id : '3',
                        segment : segment
                    }, function (err, item) {
                        expect(item).to.not.exist();
                        done();
                    });
                });

                setTimeout(function () {
                    gcExp.collect(function (err) {
                        expect(err).to.not.exist();
                    });
                }, 200);
            });
        });

        lab.test('ignores items that should not be collected -> gc == false', function (done) {
            var gc = new Gc(settings);
            var segment = 'ttltest2';
            var set = function (id, callback) {
                atableClient.set({
                    id : id,
                    segment : segment
                }, {
                    cacheme : true
                }, 50, callback);
            };

            var itemIds = ['4', '5'];

            async.each(itemIds, function (id, cb) {
                set(id, cb);
            }, function (err) {
                expect(err).to.not.exist();

                atableClient.generateRow({
                    id : '6',
                    segment : segment
                }, {
                    cacheforever : true
                }, 50, false, function (err, insertData) {

                    atableClient.client.insertOrMergeEntity(atableClient.tableName, insertData, null, function (err) {
                        expect(err).to.not.exist();

                        gc.once('collected', function (err) {
                            expect(err).to.not.exist();

                            var query = new AzureStorage.TableQuery()
                                .top(100).select('PartitionKey', 'RowKey')
                                .where('PartitionKey == ?string?', segment)
                                .and('gc == ?bool?', false);

                            gc.client.queryEntities(options.partition, query, null, function (err, result) {
                                expect(err).to.equal(null);
                                expect(result.entries).to.have.length(1);
                                done();
                            });
                        });

                        setTimeout(function () {
                            gc.collect(function (err) {
                                expect(err).to.not.exist();
                            });
                        }, 200);
                    });
                });
            });
        });

        lab.test('returns error in callback if partition (table name) is invalid', function (done) {
            var gc = new Gc(settings);
            gc.tableName = 'cache-me';
            gc.collect(function (err) {
                expect(err).to.exist();
                done();
            });
        });

        lab.test('returns null in callback if no items where found', function (done) {
            var gc = new Gc(settings);
            gc.collect(function () {
                gc.collect(function (err) {
                    expect(err).to.equal(null);
                    done();
                });
            });
        });

        lab.test('works without callback', function (done) {
            var gc = new Gc(settings);
            var fn = function () {
                gc.collect();
            };

            expect(fn).to.not.throw();
            done();
        });
    });

    lab.experiment('internals', function () {

        lab.experiment('#_createBatches', function () {
            lab.test('creates batches per PartitionKey', function (done) {
                var gc = new Gc(settings);

                var entGen = AzureStorage.TableUtilities.entityGenerator;
                var entries = [{
                        PartitionKey : entGen.String('segment1'),
                        RowKey : entGen.String('1')
                    }, {
                        PartitionKey : entGen.String('segment2'),
                        RowKey : entGen.String('1')
                    }
                ];

                gc._createBatches(entries, function (err, batches) {
                    expect(err).to.not.exist();
                    expect(batches).to.be.an.object();
                    expect(batches.segment1).to.exist();
                    expect(batches.segment2).to.exist();
                    done();
                });
            });

            lab.test('can creates batches of size 100', function (done) {
                var gc = new Gc(settings);
                var entGen = AzureStorage.TableUtilities.entityGenerator;
                var entries = [];
                for (var i = 0; i < 100; i++) {
                    entries.push({
                        PartitionKey : entGen.String('segment3'),
                        RowKey : entGen.String('entry' + i)
                    });
                }

                gc._createBatches(entries, function (err, batches) {
                    expect(err).to.not.exist();
                    expect(batches).to.be.an.object();
                    expect(batches.segment3).to.exist();
                    expect(batches.segment3.operations).to.have.length(100);

                    gc.client.executeBatch(options.partition, batches.segment3, function () {
                        done();
                    });
                });
            });

            lab.test('returns error in callback if batch insert failed', function (done) {
                var gc = new Gc(settings);

                var entGen = AzureStorage.TableUtilities.entityGenerator;
                var entries = [{
                        PartitionKey : entGen.String('segment1')
                    }, {
                        PartitionKey : entGen.String('segment2'),
                        RowKey : entGen.String('1')
                    }
                ];

                gc._createBatches(entries, function (err, batches) {
                    expect(err).to.exist();
                    done();
                });
            });
        });

        lab.experiment('#_delete', function () {
            var gc = new Gc(settings);
            lab.test('deletes item', function (done) {
                var key = {
                    id : 'delete-test-1',
                    segment : '_delete'
                };

                atableClient.set(key, {
                    cacheme : true
                }, 50, function (err) {
                    expect(err).to.not.exist();
                    var entGen = AzureStorage.TableUtilities.entityGenerator;
                    var entries = [{
                            PartitionKey : entGen.String(key.id),
                            RowKey : entGen.String(key.segment)
                        }
                    ];
                    gc._delete(entries, function (err, res) {
                        expect(err).to.not.exist();
                        console.log(res);

                        setTimeout(function () {
                            atableClient.get(key, function (err, item) {
                                expect(err).to.not.exist();
                                expect(item).to.not.exist();
                                done();
                            });
                        }, 200);
                    });
                });
            });

            lab.test('returns error in callback if batch insert failed', function (done) {
                var entGen = AzureStorage.TableUtilities.entityGenerator;
                var entries = [{
                        PartitionKey : entGen.String('segment1')
                    }, {
                        PartitionKey : entGen.String('segment2'),
                        RowKey : entGen.String('1')
                    }
                ];

                gc._delete(entries, function (err, batches) {
                    expect(err).to.exist();
                    done();
                });
            });
        });

    });

    lab.after(function (done) {
        var tableService = AzureStorage.createTableService(atableClient.settings.connection);
        tableService.deleteTableIfExists(atableClient.settings.partition, function (err) {
            expect(err).to.not.exist();
            done();
        });
    });
});
