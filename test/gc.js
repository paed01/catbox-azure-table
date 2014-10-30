/*jshint expr:true es5:true */

var AzureStorage = require('azure-storage');
var async = require('async');

var Lab = require('lab');
var AzureTable = require('..');
var Gc = AzureTable.Gc;

var expect = Lab.expect;

var lab = exports.lab = Lab.script();
// var before = Lab.before;
var beforeEach = lab.beforeEach;
var after = lab.after;
var afterEach = lab.afterEach;
var describe = lab.experiment;
var it = lab.test;

var options = {
	partition : 'unittestcachegc',
	ttl_interval : 100000
};

describe('AzureTable GC', function () {
	var atableClient = new AzureTable(options);

	describe('#ctor', function () {
		it('throws an error if not created with new', function (done) {
			var fn = function () {
				Gc();
			};

			expect(fn).to.throw(Error);
			done();
		});
	});

	describe('#start', function () {
		it('starts timer', function (done) {
			var gc = new Gc(atableClient.settings);
			gc.start();
			expect(gc._interval).to.be.an('object');
			gc.stop();
			done();
		});

		it('restarts timer if already started', function (done) {
			var gc = new Gc(atableClient.settings);
			var timer = gc.start();

			expect(timer).to.not.eql(gc.start());

			gc.stop();
			done();
		});
	});

	describe('#stop', function () {
		it('stops timer', function (done) {
			var gc = new Gc(atableClient.settings);
			gc.start();
			gc.stop();
			expect(gc._interval).to.eql(null);
			done();
		});

		it('does nothing if not started', function (done) {
			var gc = new Gc(atableClient.settings);
			gc.stop();
			expect(gc._interval).to.eql(null);
			done();
		});
	});

	describe('#collect', {
		timeout : 0
	}, function () {
		var client;
		var gc;

		beforeEach(function (done) {
			client = new AzureTable(options);
			client.start(function (err) {
				expect(err).to.not.exist;
				gc = client._gcfunc;
				gc.stop();
				done();
			});
		});

		afterEach(function (done) {
			client.stop();
			done();
		});

		it('deletes expired items', function (done) {
			var segment = 'ttltest1';
			var set = function (id, callback) {
				client.set({
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
				expect(err).to.not.exist;

				gc.collect(function (err) {
					expect(err).to.not.exist;

					var query = new AzureStorage.TableQuery()
						.top(100).select('PartitionKey', 'RowKey')
						.where('PartitionKey == ?string?', segment);

					gc.client.queryEntities(options.partition, query, null, function (err, result) {
						expect(err).to.eql(null);
						expect(result.entries).to.have.length(0);
						done();
					});
				});
			});
		});

		it('ignores items that should not be collected -> gc == false', function (done) {
			var segment = 'ttltest2';
			var set = function (id, callback) {
				client.set({
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
				expect(err).to.not.exist;

				client.generateRow({
					id : '6',
					segment : segment
				}, {
					cacheforever : true
				}, 50, false, function (err, insertData) {

					client.client.insertOrMergeEntity(client.tableName, insertData, null, function (err) {
						expect(err).to.not.exist;

						gc.collect(function (err) {
							expect(err).to.not.exist;

							var query = new AzureStorage.TableQuery()
								.top(100).select('PartitionKey', 'RowKey')
								.where('PartitionKey == ?string?', segment)
								.and('gc == ?bool?', false);

							gc.client.queryEntities(options.partition, query, null, function (err, result) {
								expect(err).to.eql(null);
								expect(result.entries).to.have.length(1);
								done();
							});
						});
					});
				});
			});
		});

		it('returns error in callback if partition (table name) is invalid', function (done) {
			gc.tableName = 'cache-me';
			gc.collect(function (err) {
				expect(err).to.exist;
				done();
			});
		});

		it('returns null in callback if no items where found', function (done) {
			gc.collect(function () {
				gc.collect(function (err) {
					expect(err).to.eql(null);
					done();
				});
			});
		});

		it('works without callback', function (done) {
			var fn = function () {
				gc.collect();
			};

			expect(fn).to.not.throw(Error);
			done();
		});
	});

	describe('internals', function () {

		describe('#_createBatches', function () {
			it('creates batches per PartitionKey', function (done) {
				var gc = new Gc(atableClient.settings);

				var entGen = AzureStorage.TableUtilities.entityGenerator;
				var entries = [{
						PartitionKey : entGen.String('segment1'),
						RowKey : entGen.String('1'),
					}, {
						PartitionKey : entGen.String('segment2'),
						RowKey : entGen.String('1'),
					}
				];

				gc._createBatches(entries, function (err, batches) {
					expect(err).to.not.exist;
					expect(batches).to.be.an('object');
					expect(batches.segment1).to.exist;
					expect(batches.segment2).to.exist;
					done();
				});
			});

			it('can creates batches of size 100', function (done) {
				var gc = new Gc(atableClient.settings);
				var entGen = AzureStorage.TableUtilities.entityGenerator;
				var entries = [];
				for (var i = 0; i < 100; i++) {
					entries.push({
						PartitionKey : entGen.String('segment3'),
						RowKey : entGen.String('entry' + i),
					});
				}

				gc._createBatches(entries, function (err, batches) {
					expect(err).to.not.exist;
					expect(batches).to.be.an('object');
					expect(batches.segment3).to.exist;
					expect(batches.segment3.operations).to.have.length(100);
                    
                    gc.client.executeBatch(options.partition, batches.segment3, function () {
                        done();
                    });
				});
			});

			it('returns error in callback if batch insert failed', function (done) {
				var gc = new Gc(atableClient.settings);

				var entGen = AzureStorage.TableUtilities.entityGenerator;
				var entries = [{
						PartitionKey : entGen.String('segment1')
					}, {
						PartitionKey : entGen.String('segment2'),
						RowKey : entGen.String('1'),
					}
				];

				gc._createBatches(entries, function (err, batches) {
					expect(err).to.exist;
					done();
				});
			});
		});

		describe('#_delete', function () {
			it('returns error in callback if batch insert failed', function (done) {
				var gc = new Gc(atableClient.settings);

				var entGen = AzureStorage.TableUtilities.entityGenerator;
				var entries = [{
						PartitionKey : entGen.String('segment1')
					}, {
						PartitionKey : entGen.String('segment2'),
						RowKey : entGen.String('1'),
					}
				];

				gc._delete(entries, function (err, batches) {
					expect(err).to.exist;
					done();
				});
			});
		});

	});

	after(function (done) {
		var tableService = AzureStorage.createTableService(atableClient.settings.connection);
		tableService.deleteTableIfExists(atableClient.settings.partition, function (err) {
			expect(err).to.not.exist;
			done();
		});
	});
});
