/*jshint expr:true es5:true */
/*eslint camelcase:0, no-unused-vars:0, handle-callback-err:0 */

var Catbox = require('catbox');
var AzureStorage = require('azure-storage');
var async = require('async');

var Lab = require('lab');
var AzureTable = require('..');

var expect = require('code').expect;

var lab = exports.lab = Lab.script();

var options = {
	connection : process.env.AZURE_TABLE_CONN,
	partition : 'unittestcache',
	ttl_interval : false
};

lab.experiment('AzureTable', function () {
	lab.experiment('#ctor', function () {
		lab.test('throws an error if not created with new', function (done) {
			var fn = function () {
				AzureTable();
			};

			expect(fn).to.throw(Error);
			done();
		});

		lab.test('instantiate without configuration throws error', function (done) {
			var fn = function () {
				var client = new AzureTable();
			};

			expect(fn).to.throw(Error);
			done();
		});

		lab.test('instantiate without partition throws an error', function (done) {
			var fn = function () {
				var client = new AzureTable({
						ttl_interval : false
					});
			};

			expect(fn).to.throw(Error, /partition/);
			done();
		});

		lab.test('instantiate without ttl_interval throws an error', function (done) {
			var fn = function () {
				var client = new AzureTable({
						partition : 'catbox'
					});
			};

			expect(fn).to.throw(Error, /ttl_interval/);
			done();
		});

		lab.test('instantiate with ttl_interval = true throws an error', function (done) {
			var fn = function () {
				var client = new AzureTable({
						partition : 'catbox',
						ttl_interval : true
					});
			};

			expect(fn).to.throw(Error, /ttl_interval/);
			done();
		});

		lab.test('instantiate with ttl_interval as string throws an error', function (done) {
			var fn = function () {
				var client = new AzureTable({
						partition : 'catbox',
						ttl_interval : 'string'
					});
			};

			expect(fn).to.throw(Error, /ttl_interval/);
			done();
		});

		lab.test('instantiate with ttl_interval as an object throws an error', function (done) {
			var fn = function () {
				var client = new AzureTable({
						partition : 'catbox',
						ttl_interval : {}
					});
			};

			expect(fn).to.throw(Error, /ttl_interval/);
			done();
		});

		lab.test('instantiate with ttl_interval as a function throws an error', function (done) {
			var fn = function () {
				var client = new AzureTable({
						partition : 'catbox',
						ttl_interval : function () {}
					});
			};

			expect(fn).to.throw(Error, /ttl_interval/);
			done();
		});

		lab.test('instantiate with ttl_interval as null throws an error', function (done) {
			var fn = function () {
				var client = new AzureTable({
						partition : 'catbox',
						ttl_interval : null
					});
			};

			expect(fn).to.throw(Error, /ttl_interval/);
			done();
		});

		lab.test('instantiate with ttl_interval as number throws no error', function (done) {
			var fn = function () {
				var client = new AzureTable({
						partition : 'catbox',
						ttl_interval : 111
					});
			};

			expect(fn).to.not.throw();
			done();
		});
	});

	lab.experiment('interface', function () {

		lab.test('get without starting returns error', function (done) {
			var client = new AzureTable(options);
			client.get({
				id : '1',
				segment : '2'
			}, function (err) {
				expect(err).to.exist();
				done();
			});
		});

		lab.test('set without starting returns error', function (done) {
			var client = new AzureTable(options);
			client.set({
				id : '1',
				segment : '2'
			}, {
				cacheme : true
			}, Infinity, function (err) {
				expect(err).to.exist();
				done();
			});
		});

		lab.test('get with invalid id returns error', function (done) {
			var client = new AzureTable(options);
			client.start(function (startErr) {
				expect(startErr).to.not.exist();

				client.get({
					id : 'two\rlines',
					segment : '2'
				}, function (err) {
					expect(err).to.exist();
					done();
				});
			});
		});

		lab.test('set with invalid id returns error', function (done) {
			var client = new AzureTable(options);
			client.start(function (startErr) {
				expect(startErr).to.not.exist();

				client.set({
					id : 'two\rlines',
					segment : '2'
				}, {
					cacheme : true
				}, Infinity, function (err) {
					expect(err).to.exist();
					done();
				});
			});
		});

		lab.test('get item that do not exist returns null', function (done) {
			var client = new AzureTable(options);
			client.start(function (startErr) {
				expect(startErr).to.not.exist();

				client.get({
					id : 'non-existing',
					segment : '2'
				}, function (err, item) {
					expect(err).to.equal(null);
					expect(item).to.equal(null);
					done();
				});
			});
		});

		lab.test('drop item that do not exist returns null', function (done) {
			var client = new AzureTable(options);
			client.start(function (startErr) {
				expect(startErr).to.not.exist();

				client.drop({
					id : 'non-existing',
					segment : '2'
				}, function (err) {
					expect(err).to.equal(null);
					done();
				});
			});
		});

		lab.test('drop item with invalid id returns error', function (done) {
			var client = new AzureTable(options);
			client.start(function (startErr) {
				expect(startErr).to.not.exist();

				client.drop({
					id : 'two\rlines',
					segment : '2'
				}, function (err) {
					expect(err).to.exist();
					done();
				});
			});
		});

		lab.test('supports empty id', function (done) {
			var client = new AzureTable(options);
			client.start(function (startErr) {
				expect(startErr).to.not.exist();

				var key = {
					id : '',
					segment : 'test'
				};
				client.set(key, '123', 1000, function (err) {
					expect(err).to.not.exist();
					client.get(key, function (err, result) {
						expect(err).to.not.exist();
						expect(result.item).to.equal('123');
						done();
					});
				});
			});
		});
	});

	lab.experiment('#start', function () {
		lab.test('returns no error', function (done) {
			var client = new Catbox.Client(AzureTable, options);
			client.start(function (err) {
				expect(err).to.not.exist();
				done();
			});
		});

		lab.test('returns no error if called twice', function (done) {
			var client = new Catbox.Client(AzureTable, options);
			client.start(function (err) {
				expect(err).to.not.exist();
				client.start(done);
			});
		});

		lab.test('returns error if partition (tableName) does not match naming convention', function (done) {
			var client = new Catbox.Client(AzureTable, {
					connection : options.connection,
					partition : 'cache-me',
					ttl_interval : false
				});
			client.start(function (err) {
				expect(err).to.exist();
				done();
			});
		});

		lab.test('throws error if connection string is wrongly formatted', function (done) {
			var client = new Catbox.Client(AzureTable, {
					connection : 'somewhere',
					partition : options.partition,
					ttl_interval : false
				});
			var fn = function () {
				client.start();
			};

			expect(fn).to.throw(Error);
			done();
		});

		lab.test('emits collected event when Gc has collected', function (done) {
			var client = new Catbox.Client(AzureTable, {
					partition : options.partition,
					ttl_interval : 100
				});
			client.start(function (err) {
				expect(err).to.not.exist();

				client.connection._gcfunc.once('collected', function (err) {
					expect(err).to.not.exist();
					done();
				});
			});
		});

		lab.test('emits collected event at least 2 times', function (done) {
			var client = new Catbox.Client(AzureTable, {
					partition : options.partition,
					ttl_interval : 100
				});
			client.start(function (err) {
				expect(err).to.not.exist();

				client.connection._gcfunc.once('collected', function (err) {
                    client.connection._gcfunc.once('collected', function (err) {
                        done();
                    });
				});
			});
		});
	});

	lab.experiment('#stop', function () {
		lab.test('returns no error', function (done) {
			var client = new Catbox.Client(AzureTable, options);
			client.start(function (err) {
				var fn = function () {
					client.stop();
				};
				expect(fn).not.to.throw();
				done();
			});

		});

		lab.test('returns no error when client isn\'t started', function (done) {
			var client = new Catbox.Client(AzureTable, options);
			var fn = function () {
				client.stop();
			};

			expect(fn).to.not.throw();
			done();
		});

		lab.test('stops Gc as well', function (done) {
			var client = new Catbox.Client(AzureTable, {
					partition : options.partition,
					ttl_interval : 100
				});
			client.start(function (err) {
                expect(client.connection._gcfunc._timer).to.not.equal(null);

                client.stop();
                expect(client.connection._gcfunc._timer).to.equal(null);
                done();
            });
        });
    });

	lab.experiment('#isReady', function () {
		lab.test('returns false if not started', function (done) {
			var client = new Catbox.Client(AzureTable, options);
			expect(client.isReady()).to.not.be.true();
			done();
		});

		lab.test('returns true if started', function (done) {
			var client = new Catbox.Client(AzureTable, options);
			client.start(function (err) {
				expect(client.isReady()).to.be.true();
				done();
			});
		});
	});

	lab.experiment('#validateSegmentName', function () {
		lab.test('returns null if validated', function (done) {
			var client = new Catbox.Client(AzureTable, options);
			expect(client.validateSegmentName('table')).to.be.null();
			done();
		});

		lab.test('returns Error if empty string', function (done) {
			var client = new Catbox.Client(AzureTable, options);
			expect(client.validateSegmentName('')).to.be.instanceOf(Error);
			done();
		});

		lab.test('returns Error if nothing passed', function (done) {
			var client = new Catbox.Client(AzureTable, options);
			expect(client.validateSegmentName()).to.be.instanceOf(Error);
			done();
		});

		lab.test('returns Error if null', function (done) {
			var client = new Catbox.Client(AzureTable, options);
			expect(client.validateSegmentName(null)).to.be.instanceOf(Error);
			done();
		});

		lab.test('returns Error if \\0', function (done) {
			var client = new Catbox.Client(AzureTable, options);
			expect(client.validateSegmentName('\0')).to.be.instanceOf(Error);
			done();
		});
	});

	lab.experiment('#set', function () {
		var client = new Catbox.Client(AzureTable, options);

		lab.before(function (done) {
			client.start(function (err) {
				expect(err).to.not.exist();
				done();
			});
		});

		lab.test('without started client returns error in callback', function (done) {
			var rawclient = new AzureTable(options);
			var d = {
				cache : true
			};
			var key = {
				id : 'item 2',
				segment : 'unittest'
			};
			rawclient.set(key, d, 10000, function (err) {
				expect(err).to.exist();
				done();
			});
		});

		lab.test('puts object in cache', function (done) {
			var d = {
				cache : true
			};
			client.set({
				id : 'item 1',
				segment : 'unittest'
			}, d, 10000, function (err) {
				expect(err).to.not.exist();
				done();
			});
		});

		lab.test('replaces object in cache', function (done) {
			var d = {
				cache : true
			};
			var key = {
				id : 'item 1 update',
				segment : 'unittest'
			};

			client.set(key, d, 10000, function (err) {
				expect(err).to.not.exist();

				var d2 = {
					update : true
				};
				client.set(key, d2, 10000, function (err) {
					client.get(key, function (err, data) {
						if (err) {
							console.log(err.stack);
						}

						expect(err).to.not.exist();

						expect(data).to.exist();
						expect(data.item).to.exist();
						expect(data.item).to.be.an.object();
						expect(data.item.cache).to.not.exist();
						expect(data.item.update).to.equal(true);

						done();
					});
				});

			});
		});

		lab.test('returns error in callback if circular json', function (done) {
			var d = {};
			d.circular = d;
			client.set({
				id : 'item 1',
				segment : 'unittest'
			}, d, 10000, function (err) {
				expect(err).to.exist();
				done();
			});
		});

		lab.test('supports empty id', function (done) {
			var d = {
				cache : true
			};
			var key = {
				id : '',
				segment : 'unittest'
			};

			client.set(key, d, 10000, function (err) {
				expect(err).to.not.exist();
				done();
			});
		});
	});

	lab.experiment('#get', function () {
		var client = new Catbox.Client(AzureTable, options);
		lab.before(function (done) {
			client.start(function (err) {
				expect(err).to.not.exist();
				done();
			});
		});

		lab.test('without started client returns error in callback', function (done) {
			var rawclient = new AzureTable(options);
			var key = {
				id : 'item 2',
				segment : 'unittest'
			};
			rawclient.get(key, function (err) {
				expect(err).to.exist();
				done();
			});
		});

		lab.test('fetches object from cache', function (done) {
			var key = {
				id : 'item 2',
				segment : 'unittest'
			};
			var d = {
				cache : true
			};
			client.set(key, d, 10000, function (err) {
				expect(err).to.not.exist();

				client.get(key, function (err, item) {
					expect(err).to.not.exist();
					expect(item).to.exist();
					done();
				});
			});
		});

		lab.test('fetches object with same data', function (done) {
			var key = {
				id : 'item 3',
				segment : 'unittest'
			};
			var d = {
				cache : 'me',
				blue : false
			};
			client.set(key, d, 10000, function (err) {
				expect(err).to.not.exist();

				client.get(key, function (err, data) {
					expect(err).to.not.exist();

					expect(data).to.exist();
					expect(data.item).to.exist();
					expect(data.item).to.be.an.object();
					expect(data.item.cache).to.equal(d.cache);
					expect(data.item.blue).to.equal(false);

					done();
				});
			});
		});

		lab.test('with non-existing key id returns nothing', function (done) {
			var key = {
				id : 'no-item 1',
				segment : 'unittest'
			};
			client.get(key, function (err, data) {
				expect(err).to.not.exist();
				expect(data).to.not.exist();
				done();
			});
		});

		lab.test('with non-existing key segment returns nothing', function (done) {
			var key = {
				id : 'no-item 1',
				segment : 'unittest-non-existing'
			};
			client.get(key, function (err, data) {
				expect(err).to.not.exist();
				expect(data).to.not.exist();
				done();
			});
		});

		lab.test('with non-json data in table returns error in callback', function (done) {
			var key = {
				id : 'Wrongly formatted 1',
				segment : 'unittest'
			};
			var entGen = AzureStorage.TableUtilities.entityGenerator;
			var insertData = {
				PartitionKey : entGen.String(key.segment),
				RowKey : entGen.String(key.id),
				item : entGen.String('[Object weee]'),
				ttl : entGen.Int64(10)
			};

			client.connection.client.insertOrMergeEntity(client.connection.tableName, insertData, function (err) {
				expect(err).to.not.exist();

				client.get(key, function (err, data) {
					expect(err).to.exist();
					expect(err.message).to.equal('Bad value content');
					done();
				});
			});
		});

		lab.test('returns stored as timestamp', function (done) {
			var key = {
				id : 'item 2 with ts',
				segment : 'unittest'
			};
			var d = {
				cache : true
			};
			client.set(key, d, 10000, function (err) {
				expect(err).to.not.exist();

				client.get(key, function (err, data) {
					expect(err).to.not.exist();

					expect(data.stored).to.not.be.instanceOf(Date);

					done();
				});
			});
		});

		lab.test('returns ttl as number', function (done) {
			var key = {
				id : 'item 2 with ts',
				segment : 'unittest'
			};
			var d = {
				cache : true
			};
			client.set(key, d, 10000, function (err) {
				expect(err).to.not.exist();

				client.get(key, function (err, data) {
					expect(err).to.not.exist();

					expect(data.ttl).to.be.a.number();

					done();
				});
			});
		});

		lab.test('supports empty id', function (done) {
			var d = {
				empty_id : true
			};
			var key = {
				id : '',
				segment : 'unittest'
			};

			client.set(key, d, 10000, function (err) {
				expect(err).to.not.exist();

				client.get(key, function (err, data) {
					expect(err).to.not.exist();

					expect(data).to.exist();
					expect(data.item).to.exist();
					expect(data.item).to.be.an.object();
					expect(data.item.empty_id).to.equal(d.empty_id);

					done();
				});
			});
		});
	});

	lab.experiment('#drop', function () {
		var client = new Catbox.Client(AzureTable, options);
		lab.before(function (done) {
			client.start(function (err) {
				expect(err).to.not.exist();
				done();
			});
		});

		lab.test('without started client returns error in callback', function (done) {
			var rawclient = new AzureTable(options);
			var key = {
				id : 'item 4',
				segment : 'unittest'
			};
			rawclient.drop(key, function (err) {
				expect(err).to.exist();
				done();
			});
		});

		lab.test('drops object from cache', function (done) {
			var key = {
				id : 'item 4',
				segment : 'unittest'
			};
			var d = {
				cache : true
			};
			client.set(key, d, 10000, function (err) {
				expect(err).to.not.exist();

				client.drop(key, function (err) {
					expect(err).to.not.exist();
					client.get(key, function (err, data) {
						expect(err).to.not.exist();
						expect(data).to.not.exist();
						done();
					});
				});
			});
		});

		lab.test('with non-existing key id returns nothing', function (done) {
			var key = {
				id : 'no-item 2',
				segment : 'unittest'
			};
			client.drop(key, function (err) {
				expect(err).to.not.exist();
				done();
			});
		});

		lab.test('with non-existing segment returns nothing', function (done) {
			var key = {
				id : 'no-item 2',
				segment : 'unittest-non-existing'
			};
			client.get(key, function (err) {
				expect(err).to.not.exist();
				done();
			});
		});

	});

	lab.after(function (done) {
		var client = new AzureTable(options);
		client.start(function () {
			client.client.deleteTableIfExists(options.partition, function (err) {
				expect(err).to.not.exist();
				done();
			});
		});
	});
});
