'use strict';

const AzureStorage = require('azure-storage');
const async = require('async');
const Hoek = require('hoek');

const Lab = require('lab');
const AzureTable = require('..');
const Gc = AzureTable.Gc;

const expect = require('code').expect;

const lab = exports.lab = Lab.script();

const options = {
  connection: process.env.AZURE_TABLE_CONN,
  partition: 'unittestcachegc',
  ttl_interval: 0
};

lab.experiment('AzureTable GC', () => {
  const atableClient = new AzureTable(options);
  const settings = Hoek.clone(atableClient.settings);

  settings.ttl_interval = 5000;

  lab.before((done) => {
    atableClient.start(done);
  });

  lab.experiment('#ctor', () => {
    lab.test('throws an error if not created with new', (done) => {
      function fn() {
        Gc();
      }

      expect(fn).to.throw(Error);
      done();
    });
  });

  lab.experiment('#start', () => {
    lab.test('starts timer', (done) => {
      const gc = new Gc(settings);
      gc.start((err, timer) => {
        if (err) return done(err);
        expect(timer).to.be.an.object();
        gc.stop();
        done();
      });
    });

    lab.test('restarts timer if already started', (done) => {
      const gc = new Gc(settings);
      gc.start((err1, timer1) => {
        gc.start((err2, timer2) => {
          expect(timer1).to.not.equal(timer2);
          gc.stop();
          done();
        });
      });
    });
  });

  lab.experiment('#stop', () => {
    lab.test('stops timer', (done) => {
      const gc = new Gc(settings);
      gc.start();
      gc.stop();
      expect(gc._timer).to.equal(null);
      done();
    });

    lab.test('does nothing if not started', (done) => {
      const gc = new Gc(settings);
      gc.stop();
      expect(gc._timer).to.equal(null);
      done();
    });
  });

  lab.experiment('#collect', () => {
    lab.test('deletes expired items', (done) => {
      const gcExp = new Gc(settings);
      let segment = 'ttltest1';

      function set(id, callback) {
        atableClient.set({
          id: id,
          segment: segment
        }, {
          cacheme: true
        }, 50, callback);
      }

      let itemIds = ['1', '2', '3'];

      async.eachSeries(itemIds, (id, cb) => {
        set(id, cb);
      }, (err) => {
        expect(err).to.not.exist();

        gcExp.once('collected', (collectErr) => {
          expect(collectErr).to.not.exist();

          atableClient.get({
            id: '3',
            segment: segment
          }, (getErr, item) => {
            expect(item).to.not.exist();
            done();
          });
        });

        setTimeout(() => {
          gcExp.collect((collectErr) => {
            expect(collectErr).to.not.exist();
          });
        }, 200);
      });
    });

    lab.test('ignores items that should not be collected -> gc == false', (done) => {
      const gc = new Gc(settings);
      const segment = 'ttltest2';

      function set(id, callback) {
        atableClient.set({
          id: id,
          segment: segment
        }, {
          cacheme: true
        }, 50, callback);
      }

      let itemIds = ['4', '5'];

      async.eachSeries(itemIds, (id, cb) => {
        set(id, cb);
      }, (err) => {
        if (err) return done(err);

        atableClient.generateRow({
          id: '6',
          segment: segment
        }, {
          cacheforever: true
        }, 50, false, (err1, insertData) => {
          if (err1) return done(err);

          atableClient.client.insertOrMergeEntity(atableClient.tableName, insertData, null, (err2) => {
            if (err2) return done(err2);

            gc.once('collected', (collectErr) => {
              expect(collectErr).to.not.exist();

              let query = new AzureStorage.TableQuery()
                .top(100).select('PartitionKey', 'RowKey')
                .where('PartitionKey == ?string?', segment)
                .and('gc == ?bool?', false);

              gc.client.queryEntities(options.partition, query, null, (qerr, result) => {
                expect(qerr).to.equal(null);
                expect(result.entries).to.have.length(1);
                done();
              });
            });

            setTimeout(() => {
              gc.collect((collectErr) => {
                expect(collectErr).to.not.exist();
              });
            }, 200);
          });
        });
      });
    });

    lab.test('returns error in callback if partition (table name) is invalid', (done) => {
      const gc = new Gc(settings);
      gc.tableName = 'cache-me';
      gc.collect((err) => {
        expect(err).to.exist();
        done();
      });
    });

    lab.test('returns null in callback if no items where found', (done) => {
      const gc = new Gc(settings);
      gc.collect(() => {
        gc.collect(done);
      });
    });

    lab.test('works without callback', (done) => {
      const gc = new Gc(settings);
      function fn() {
        gc.collect();
      }

      expect(fn).to.not.throw();
      done();
    });
  });

  lab.experiment('internals', () => {

    lab.experiment('#_createDeleteBatches', () => {

      lab.test('creates batches per PartitionKey', (done) => {
        const entGen = AzureStorage.TableUtilities.entityGenerator;
        const entries = [{
          PartitionKey: entGen.String('segment1'),
          RowKey: entGen.String('1')
        }, {
          PartitionKey: entGen.String('segment2'),
          RowKey: entGen.String('1')
        }];

        let batches = Gc._createDeleteBatches(entries);
        expect(batches).to.be.an.object();
        expect(batches.segment1).to.exist();
        expect(batches.segment2).to.exist();
        done();
      });

      lab.test('can creates batches of size 100', (done) => {
        const gc = new Gc(settings);
        const entGen = AzureStorage.TableUtilities.entityGenerator;
        let entries = [];
        for (let i = 0; i < 100; i++) {
          entries.push({
            PartitionKey: entGen.String('segment3'),
            RowKey: entGen.String(`entry ${i}`)
          });
        }

        let batches = Gc._createDeleteBatches(entries);
        expect(batches).to.be.an.object();
        expect(batches.segment3).to.exist();
        expect(batches.segment3.operations).to.have.length(100);

        gc.client.executeBatch(options.partition, batches.segment3, () => {
          return done();
        });
      });

      lab.test('returns error in batch if batch insert failed', (done) => {
        const entGen = AzureStorage.TableUtilities.entityGenerator;
        const entries = [{
          PartitionKey: entGen.String('segment1')
        }, {
          PartitionKey: entGen.String('segment2'),
          RowKey: entGen.String('1')
        }];

        let batches = Gc._createDeleteBatches(entries);
        expect(batches.segment1.error).to.exist();
        done();
      });
    });

    lab.experiment('#_delete', () => {
      const gc = new Gc(settings);

      lab.test('deletes item', (done) => {
        const key = {
          id: 'delete-test-1',
          segment: '_delete'
        };

        atableClient.set(key, {
          cacheme: true
        }, 50, (err) => {
          expect(err).to.not.exist();
          const entGen = AzureStorage.TableUtilities.entityGenerator;
          let entries = [{
            PartitionKey: entGen.String(key.id),
            RowKey: entGen.String(key.segment)
          }];
          gc._delete(entries, (delErr) => {
            if (delErr) return done(delErr);

            setTimeout(() => {
              atableClient.get(key, (getErr, item) => {
                expect(getErr).to.not.exist();
                expect(item).to.not.exist();
                done();
              });
            }, 200);
          });
        });
      });

      lab.test('emits delete-error if batch insert failed', (done) => {
        const entGen = AzureStorage.TableUtilities.entityGenerator;
        const entries = [{
          PartitionKey: entGen.String('segment1')
        }, {
          PartitionKey: entGen.String('segment2'),
          RowKey: entGen.String('1')
        }];

        gc.once('delete-error', (err) => {
          expect(err).to.exist();
          done();
        });

        gc._delete(entries, (err) => {
          expect(err).to.not.exist();
        });
      });
    });

  });

  lab.after((done) => {
    const tableService = AzureStorage.createTableService(atableClient.settings.connection);
    tableService.deleteTableIfExists(atableClient.settings.partition, done);
  });
});
