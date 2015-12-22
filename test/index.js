/* eslint no-new:0 */
'use strict';

const Catbox = require('catbox');
const AzureStorage = require('azure-storage');

const Lab = require('lab');
const AzureTable = require('..');

const expect = require('code').expect;

const lab = exports.lab = Lab.script();

const options = {
  connection: process.env.AZURE_TABLE_CONN,
  partition: 'unittestcache',
  ttl_interval: false
};

lab.experiment('AzureTable', () => {
  lab.experiment('#ctor', () => {
    lab.test('throws an error if not created with new', (done) => {
      function fn() {
        AzureTable();
      }

      expect(fn).to.throw(Error);
      done();
    });

    lab.test('instantiate without configuration throws error', (done) => {
      function fn() {
        /* eslint no-new:0 */
        new AzureTable();
      }

      expect(fn).to.throw(Error);
      done();
    });

    lab.test('instantiate without partition throws an error', (done) => {
      function fn() {
        new AzureTable({
          ttl_interval: false
        });
      }

      expect(fn).to.throw(Error, /partition/);
      done();
    });

    lab.test('instantiate without ttl_interval throws an error', (done) => {
      function fn() {
        new AzureTable({
          partition: 'catbox'
        });
      }

      expect(fn).to.throw(Error, /ttl_interval/);
      done();
    });

    lab.test('instantiate with ttl_interval = true throws an error', (done) => {
      function fn() {
        new AzureTable({
          partition: 'catbox',
          ttl_interval: true
        });
      }

      expect(fn).to.throw(Error, /ttl_interval/);
      done();
    });

    lab.test('instantiate with ttl_interval as string throws an error', (done) => {
      function fn() {
        new AzureTable({
          partition: 'catbox',
          ttl_interval: 'string'
        });
      }

      expect(fn).to.throw(Error, /ttl_interval/);
      done();
    });

    lab.test('instantiate with ttl_interval as an object throws an error', (done) => {
      function fn() {
        new AzureTable({
          partition: 'catbox',
          ttl_interval: {}
        });
      }

      expect(fn).to.throw(Error, /ttl_interval/);
      done();
    });

    lab.test('instantiate with ttl_interval as a function throws an error', (done) => {
      function fn() {
        new AzureTable({
          partition: 'catbox',
          ttl_interval: () => {}
        });
      }

      expect(fn).to.throw(Error, /ttl_interval/);
      done();
    });

    lab.test('instantiate with ttl_interval as null throws an error', (done) => {
      function fn() {
        new AzureTable({
          partition: 'catbox',
          ttl_interval: null
        });
      }

      expect(fn).to.throw(Error, /ttl_interval/);
      done();
    });

    lab.test('instantiate with ttl_interval as number throws no error', (done) => {
      function fn() {
        new AzureTable({
          partition: 'catbox',
          ttl_interval: 111
        });
      }

      expect(fn).to.not.throw();
      done();
    });
  });

  lab.experiment('interface', () => {

    lab.test('get without starting returns error', (done) => {
      const client = new AzureTable(options);
      client.get({
        id: '1',
        segment: '2'
      }, (err) => {
        expect(err).to.exist();
        done();
      });
    });

    lab.test('set without starting returns error', (done) => {
      const client = new AzureTable(options);
      client.set({
        id: '1',
        segment: '2'
      }, {
        cacheme: true
      }, Infinity, (err) => {
        expect(err).to.exist();
        done();
      });
    });

    lab.test('get with invalid id returns error', (done) => {
      const client = new AzureTable(options);
      client.start((startErr) => {
        if (startErr) return done(startErr);

        client.get({
          id: 'two\rlines',
          segment: '2'
        }, (err) => {
          expect(err).to.exist();
          done();
        });
      });
    });

    lab.test('set with invalid id returns error', (done) => {
      const client = new AzureTable(options);
      client.start((startErr) => {
        if (startErr) return done(startErr);

        client.set({
          id: 'two\rlines',
          segment: '2'
        }, {
          cacheme: true
        }, Infinity, (err) => {
          expect(err).to.exist();
          done();
        });
      });
    });

    lab.test('get item that do not exist returns null', (done) => {
      const client = new AzureTable(options);
      client.start((startErr) => {
        if (startErr) return done(startErr);

        client.get({
          id: 'non-existing',
          segment: '2'
        }, (err, item) => {
          if (err) return done(err);
          expect(item).to.equal(null);
          done();
        });
      });
    });

    lab.test('drop item that do not exist returns null', (done) => {
      const client = new AzureTable(options);
      client.start((startErr) => {
        if (startErr) return done(startErr);

        client.drop({
          id: 'non-existing',
          segment: '2'
        }, done);
      });
    });

    lab.test('drop item with invalid id returns error', (done) => {
      const client = new AzureTable(options);
      client.start((startErr) => {
        if (startErr) return done(startErr);

        client.drop({
          id: 'two\rlines',
          segment: '2'
        }, (err) => {
          expect(err).to.exist();
          done();
        });
      });
    });

    lab.test('supports empty id', (done) => {
      const client = new AzureTable(options);
      client.start((startErr) => {
        if (startErr) return done(startErr);

        const key = {
          id: '',
          segment: 'test'
        };
        client.set(key, '123', 1000, (setErr) => {
          if (setErr) return done(setErr);

          client.get(key, (err, result) => {
            if (err) return done(err);
            expect(result.item).to.equal('123');
            done();
          });
        });
      });
    });
  });

  lab.experiment('#start', () => {
    lab.test('returns no error', (done) => {
      const client = new Catbox.Client(AzureTable, options);
      client.start(done);
    });

    lab.test('returns no error if called twice', (done) => {
      const client = new Catbox.Client(AzureTable, options);
      client.start((err) => {
        if (err) return done(err);
        client.start(done);
      });
    });

    lab.test('returns error if partition (tableName) does not match naming convention', (done) => {
      const client = new Catbox.Client(AzureTable, {
        connection: options.connection,
        partition: 'cache-me',
        ttl_interval: false
      });
      client.start((err) => {
        expect(err).to.exist();
        done();
      });
    });

    lab.test('throws error if connection string is wrongly formatted', (done) => {
      const client = new Catbox.Client(AzureTable, {
        connection: 'somewhere',
        partition: options.partition,
        ttl_interval: false
      });
      function fn() {
        client.start();
      }

      expect(fn).to.throw(Error);
      done();
    });

    lab.test('emits collected event when Gc has collected', (done) => {
      const client = new Catbox.Client(AzureTable, {
        partition: options.partition,
        ttl_interval: 100
      });
      client.start((startErr) => {
        if (startErr) return done(startErr);

        client.connection._gcfunc.once('collected', (err) => {
          expect(err).to.not.exist();
          done();
        });
      });
    });

    lab.test('emits collected event at least 2 times', (done) => {
      const client = new Catbox.Client(AzureTable, {
        partition: options.partition,
        ttl_interval: 100
      });
      client.start((err) => {
        expect(err).to.not.exist();

        client.connection._gcfunc.once('collected', () => {
          client.connection._gcfunc.once('collected', () => {
            done();
          });
        });
      });
    });
  });

  lab.experiment('#stop', () => {
    lab.test('twice returns no error', (done) => {
      const client = new Catbox.Client(AzureTable, options);
      client.start((err) => {
        if (err) return done(err);

        function fn() {
          client.stop();
        }
        expect(fn).not.to.throw();
        done();
      });

    });

    lab.test('returns no error when client isn\'t started', (done) => {
      const client = new Catbox.Client(AzureTable, options);
      function fn() {
        client.stop();
      }

      expect(fn).to.not.throw();
      done();
    });

    lab.test('stops Gc as well', (done) => {
      const client = new Catbox.Client(AzureTable, {
        partition: options.partition,
        ttl_interval: 100
      });
      client.start((err) => {
        if (err) return done(err);
        expect(client.connection._gcfunc._timer).to.not.equal(null);

        client.stop();
        expect(client.connection._gcfunc._timer).to.equal(null);
        done();
      });
    });
  });

  lab.experiment('#isReady', () => {
    lab.test('returns false if not started', (done) => {
      const client = new Catbox.Client(AzureTable, options);
      expect(client.isReady()).to.not.be.true();
      done();
    });

    lab.test('returns true if started', (done) => {
      const client = new Catbox.Client(AzureTable, options);
      client.start((err) => {
        if (err) return done(err);

        expect(client.isReady()).to.be.true();
        done();
      });
    });
  });

  lab.experiment('#validateSegmentName', () => {
    lab.test('returns null if validated', (done) => {
      const client = new Catbox.Client(AzureTable, options);
      expect(client.validateSegmentName('table')).to.be.null();
      done();
    });

    lab.test('returns Error if empty string', (done) => {
      const client = new Catbox.Client(AzureTable, options);
      expect(client.validateSegmentName('')).to.be.instanceOf(Error);
      done();
    });

    lab.test('returns Error if nothing passed', (done) => {
      const client = new Catbox.Client(AzureTable, options);
      expect(client.validateSegmentName()).to.be.instanceOf(Error);
      done();
    });

    lab.test('returns Error if null', (done) => {
      const client = new Catbox.Client(AzureTable, options);
      expect(client.validateSegmentName(null)).to.be.instanceOf(Error);
      done();
    });

    lab.test('returns Error if \\0', (done) => {
      const client = new Catbox.Client(AzureTable, options);
      expect(client.validateSegmentName('\0')).to.be.instanceOf(Error);
      done();
    });
  });

  lab.experiment('#set', () => {
    const client = new Catbox.Client(AzureTable, options);

    lab.before(client.start.bind(client));

    lab.test('without started client returns error in callback', (done) => {
      const rawclient = new AzureTable(options);
      const d = {
        cache: true
      };
      const key = {
        id: 'item 2',
        segment: 'unittest'
      };
      rawclient.set(key, d, 10000, (err) => {
        expect(err).to.exist();
        done();
      });
    });

    lab.test('puts object in cache', (done) => {
      const d = {
        cache: true
      };
      client.set({
        id: 'item 1',
        segment: 'unittest'
      }, d, 10000, done);
    });

    lab.test('replaces object in cache', (done) => {
      const d = {
        cache: true
      };
      const key = {
        id: 'item 1 update',
        segment: 'unittest'
      };

      client.set(key, d, 10000, (setErr) => {
        if (setErr) return done(setErr);

        const d2 = {
          update: true
        };
        client.set(key, d2, 10000, () => {

          client.get(key, (err, data) => {
            if (err) return done(err);

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

    lab.test('returns error in callback if circular json', (done) => {
      let d = {};
      d.circular = d;
      client.set({
        id: 'item 1',
        segment: 'unittest'
      }, d, 10000, (err) => {
        expect(err).to.exist();
        done();
      });
    });

    lab.test('supports empty id', (done) => {
      const d = {
        cache: true
      };
      const key = {
        id: '',
        segment: 'unittest'
      };

      client.set(key, d, 10000, done);
    });
  });

  lab.experiment('#get', () => {
    const client = new Catbox.Client(AzureTable, options);

    lab.before(client.start.bind(client));

    lab.test('without started client returns error in callback', (done) => {
      const rawclient = new AzureTable(options);
      const key = {
        id: 'item 2',
        segment: 'unittest'
      };
      rawclient.get(key, (err) => {
        expect(err).to.exist();
        done();
      });
    });

    lab.test('fetches object from cache', (done) => {
      const key = {
        id: 'item 2',
        segment: 'unittest'
      };
      const d = {
        cache: true
      };
      client.set(key, d, 10000, (setErr) => {
        if (setErr) return done(setErr);

        client.get(key, (err, item) => {
          if (err) return done(err);

          expect(item).to.exist();
          done();
        });
      });
    });

    lab.test('fetches object with same data', (done) => {
      const key = {
        id: 'item 3',
        segment: 'unittest'
      };
      const d = {
        cache: 'me',
        blue: false
      };
      client.set(key, d, 10000, (setErr) => {
        if (setErr) return done(setErr);

        client.get(key, (err, data) => {
          if (err) return done(err);

          expect(data).to.exist();
          expect(data.item).to.exist();
          expect(data.item).to.be.an.object();
          expect(data.item.cache).to.equal(d.cache);
          expect(data.item.blue).to.equal(false);

          done();
        });
      });
    });

    lab.test('with non-existing key id returns nothing', (done) => {
      const key = {
        id: 'no-item 1',
        segment: 'unittest'
      };
      client.get(key, (err, data) => {
        if (err) return done(err);
        expect(data).to.not.exist();
        done();
      });
    });

    lab.test('with non-existing key segment returns nothing', (done) => {
      const key = {
        id: 'no-item 1',
        segment: 'unittest-non-existing'
      };
      client.get(key, (err, data) => {
        if (err) return done(err);
        expect(data).to.not.exist();
        done();
      });
    });

    lab.test('with non-json data in table returns error in callback', (done) => {
      const key = {
        id: 'Wrongly formatted 1',
        segment: 'unittest'
      };
      const entGen = AzureStorage.TableUtilities.entityGenerator;
      const insertData = {
        PartitionKey: entGen.String(key.segment),
        RowKey: entGen.String(key.id),
        item: entGen.String('[Object weee]'),
        ttl: entGen.Int64(10)
      };

      client.connection.client.insertOrMergeEntity(client.connection.tableName, insertData, (err) => {
        if (err) return done(err);

        client.get(key, (getErr) => {
          expect(getErr).to.exist();
          expect(getErr.message).to.equal('Bad value content');
          done();
        });
      });
    });

    lab.test('returns stored as timestamp', (done) => {
      const key = {
        id: 'item 2 with ts',
        segment: 'unittest'
      };
      const d = {
        cache: true
      };
      client.set(key, d, 10000, (setErr) => {
        if (setErr) return done(setErr);

        client.get(key, (err, data) => {
          if (err) return done(err);

          expect(data.stored).to.not.be.instanceOf(Date);

          done();
        });
      });
    });

    lab.test('returns ttl as number', (done) => {
      const key = {
        id: 'item 2 with ts',
        segment: 'unittest'
      };
      const d = {
        cache: true
      };
      client.set(key, d, 10000, (setErr) => {
        if (setErr) return done(setErr);

        client.get(key, (err, data) => {
          if (err) return done(err);

          expect(data.ttl).to.be.a.number();

          done();
        });
      });
    });

    lab.test('supports empty id', (done) => {
      const d = {
        empty_id: true
      };
      const key = {
        id: '',
        segment: 'unittest'
      };

      client.set(key, d, 10000, (setErr) => {
        if (setErr) return done(setErr);

        client.get(key, (err, data) => {
          if (err) return done(err);

          expect(data).to.exist();
          expect(data.item).to.exist();
          expect(data.item).to.be.an.object();
          expect(data.item.empty_id).to.equal(d.empty_id);

          done();
        });
      });
    });
  });

  lab.experiment('#drop', () => {
    const client = new Catbox.Client(AzureTable, options);

    lab.before(client.start.bind(client));

    lab.test('without started client returns error in callback', (done) => {
      const rawclient = new AzureTable(options);
      const key = {
        id: 'item 4',
        segment: 'unittest'
      };
      rawclient.drop(key, (err) => {
        expect(err).to.exist();
        done();
      });
    });

    lab.test('drops object from cache', (done) => {
      const key = {
        id: 'item 4',
        segment: 'unittest'
      };
      const d = {
        cache: true
      };
      client.set(key, d, 10000, (setErr) => {
        if (setErr) return done(setErr);

        client.drop(key, (dropErr) => {
          if (dropErr) return done(dropErr);

          client.get(key, (err, data) => {
            if (err) return done(err);

            expect(data).to.not.exist();
            done();
          });
        });
      });
    });

    lab.test('with non-existing key id returns nothing', (done) => {
      const key = {
        id: 'no-item 2',
        segment: 'unittest'
      };
      client.drop(key, done);
    });

    lab.test('with non-existing segment returns nothing', (done) => {
      const key = {
        id: 'no-item 2',
        segment: 'unittest-non-existing'
      };
      client.get(key, done);
    });

  });

  lab.after((done) => {
    const client = new AzureTable(options);
    client.start(() => {
      client.client.deleteTableIfExists(options.partition, done);
    });
  });
});
