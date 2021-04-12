'use strict';

const AzureStorage = require('azure-storage');
const AzureTable = require('..');
const TableClient = require('../lib/TableClient');
const {Client} = require('@hapi/catbox');

const {after, before, describe, fail, it} = exports.lab = require('@hapi/lab').script();

const options = {
  connection: process.env.AZURE_TABLE_CONN,
  partition: 'unittestcache',
  ttl_interval: false
};

const {connection, partition} = options;

describe('AzureTable', () => {
  after(async () => {
    await TableClient(connection, partition).deleteTable();
  });

  describe('#ctor', () => {
    it('instantiate without configuration throws error', () => {
      function fn() {
        new AzureTable();
      }

      expect(fn).to.throw(Error);
    });

    it('instantiate without partition throws an error', () => {
      function fn() {
        new AzureTable({
          ttl_interval: false
        });
      }

      expect(fn).to.throw(Error, /partition/);
    });

    it('instantiate without connection string defaults to development storage', () => {
      const client = AzureTable({
        partition,
        ttl_interval: false
      });

      expect(client.getClient().connectionString).to.equal('UseDevelopmentStorage=true;DevelopmentStorageProxyUri=http://127.0.0.1;');
    });

    it('instantiate without ttl_interval throws an error', () => {
      function fn() {
        new AzureTable({
          partition: 'catbox'
        });
      }

      expect(fn).to.throw(Error, /ttl_interval/);
    });

    it('instantiate with ttl_interval = true throws an error', () => {
      function fn() {
        new AzureTable({
          partition: 'catbox',
          ttl_interval: true
        });
      }

      expect(fn).to.throw(Error, /ttl_interval/);
    });

    it('instantiate with ttl_interval as string throws an error', () => {
      function fn() {
        new AzureTable({
          partition: 'catbox',
          ttl_interval: 'string'
        });
      }

      expect(fn).to.throw(Error, /ttl_interval/);
    });

    it('instantiate with ttl_interval as an object throws an error', () => {
      function fn() {
        new AzureTable({
          partition: 'catbox',
          ttl_interval: {}
        });
      }

      expect(fn).to.throw(Error, /ttl_interval/);
    });

    it('instantiate with ttl_interval as a function throws an error', () => {
      function fn() {
        new AzureTable({
          partition: 'catbox',
          ttl_interval: () => {}
        });
      }

      expect(fn).to.throw(Error, /ttl_interval/);
    });

    it('instantiate with ttl_interval as null throws an error', () => {
      function fn() {
        new AzureTable({
          partition: 'catbox',
          ttl_interval: null
        });
      }

      expect(fn).to.throw(Error, /ttl_interval/);
    });

    it('instantiate with ttl_interval as number throws no error', () => {
      function fn() {
        new AzureTable({
          partition: 'catbox',
          ttl_interval: 111
        });
      }

      expect(fn).to.not.throw();
    });
  });

  describe('start()', () => {
    it('returns no error', async () => {
      const client = new Client(AzureTable, options);
      await client.start();
      expect(client.isReady()).to.equal(true);
    });

    it('second call to start is ignored if started', async () => {
      const client = new Client(AzureTable, options);
      await client.start();
      await client.start();
      expect(client.isReady()).to.equal(true);
    });

    it('throws if partition (tableName) does not match naming convention', () => {
      try {
        new Client(AzureTable, {
          connection,
          partition: 'cache-me',
          ttl_interval: false
        });
      } catch (err) {
        expect(err).to.be.an.error(/Table name format/i);
      }
    });

    it('throws error if connection string is malformatted', async () => {
      const client = new Client(AzureTable, {
        connection: 'somewhere',
        partition,
        ttl_interval: false
      });

      try {
        await client.start();
      } catch (err) {
        expect(err).to.be.an.error(/Connection strings/i);
      }
    });

  });

  describe('interface', () => {
    it('get without starting returns error', () => {
      const client = new AzureTable(options);
      return client.get({
        id: '1',
        segment: '2'
      }).then(() => {
        fail('Should not be ok');
      }).catch((err) => {
        expect(err).to.be.an.error(/Connection not started/i);
      });
    });

    it('set without starting returns error', async () => {
      const client = new AzureTable(options);
      return client.set({
        id: '1',
        segment: '2'
      }, {
        cacheme: true
      }, Infinity).then(() => {
        fail('Should not be ok');
      }).catch((err) => {
        expect(err).to.be.an.error(/Connection not started/i);
      });
    });

    it('get with invalid id returns error', async () => {
      const client = new AzureTable(options);
      return client.start().then(() => {
        client.get({
          id: 'two\rlines',
          segment: '2'
        }).then(() => {
          fail('Should not be ok');
        }).catch((err) => {
          expect(err).to.be.an.error(/invalid/i);
        });
      });
    });

    it('set with invalid id returns error', async () => {
      const client = new AzureTable(options);
      return client.start().then(() => {
        client.set({
          id: 'two\rlines',
          segment: '2'
        }, {
          cacheme: true
        }, Infinity).then(() => {
          fail('Should not be ok');
        }).catch((err) => {
          expect(err).to.be.an.error(/invalid/i);
        });
      });
    });

    it('get non-existing item returns null', async () => {
      const client = new AzureTable(options);
      await client.start();
      const data = await client.get({
        id: 'non-existing',
        segment: '2'
      });
      expect(data).to.equal(null);
    });

    it('drop non-existing item returns null', async () => {
      const client = new AzureTable(options);
      await client.start();

      const data = await client.drop({
        id: 'non-existing',
        segment: '2'
      });

      expect(data).to.equal(null);
    });

    it('drop item with invalid id returns error', async () => {
      const client = new AzureTable(options);
      return client.start().then(() => {
        client.drop({
          id: 'two\rlines',
          segment: '2'
        }).then(() => {
          fail('Should not be ok');
        }).catch((err) => {
          expect(err).to.be.an.error();
        });
      });
    });

    it('supports empty id', async () => {
      const client = new AzureTable(options);
      await client.start();

      const key = {
        id: '',
        segment: 'test'
      };
      await client.set(key, '123', 1000);
      const data = await client.get(key);
      expect(data.item).to.equal('123');
    });
  });

  describe('stop()', () => {
    it('twice returns no error', async () => {
      const client = new Client(AzureTable, options);
      await client.start();
      client.stop();
      client.stop();
      expect(client.isReady()).to.be.false();
    });

    it('can be stopped even if not started', () => {
      const client = new Client(AzureTable, options);
      function fn() {
        client.stop();
      }

      expect(fn).to.not.throw();
    });

    it('can be started again', async () => {
      const client = new Client(AzureTable, options);
      await client.start();
      client.stop();
      await client.start();
      expect(client.isReady()).to.be.true();
    });

    it('stops Gc as well', async () => {
      const client = new Client(AzureTable, {
        connection,
        partition,
        ttl_interval: 100
      });
      await client.start();

      expect(client.connection.gc.isReady()).to.be.true();
      client.stop();
      expect(client.connection.gc.isReady()).to.be.false();
    });
  });

  describe('isReady()', () => {
    it('returns false if not started', () => {
      const client = new Client(AzureTable, options);
      expect(client.isReady()).to.not.be.true();
    });

    it('returns true if started', async () => {
      const client = new Client(AzureTable, options);
      await client.start();
      expect(client.isReady()).to.be.true();
    });
  });

  describe('validateSegmentName()', () => {
    it('returns null if validated', () => {
      const client = new Client(AzureTable, options);
      expect(client.validateSegmentName('table')).to.be.null();
    });

    it('returns Error if empty string', () => {
      const client = new Client(AzureTable, options);
      expect(client.validateSegmentName('')).to.be.instanceOf(Error);
    });

    it('returns Error if nothing passed', () => {
      const client = new Client(AzureTable, options);
      expect(client.validateSegmentName()).to.be.instanceOf(Error);
    });

    it('returns Error if null', () => {
      const client = new Client(AzureTable, options);
      expect(client.validateSegmentName(null)).to.be.instanceOf(Error);
    });

    it('returns Error if \\0', () => {
      const client = new Client(AzureTable, options);
      expect(client.validateSegmentName('\0')).to.be.instanceOf(Error);
    });
  });

  describe('set()', () => {
    let client;

    before(async () => {
      client = new Client(AzureTable, options);
      await client.start();
    });

    it('without started client returns error in callback', async () => {
      const rawclient = new AzureTable(options);
      const d = {
        cache: true
      };
      const key = {
        id: 'item 2',
        segment: 'unittest'
      };
      return rawclient.set(key, d, 10000).then(() => {
        fail('Should not be ok');
      }).catch((err) => {
        expect(err).to.be.an.error(/not started/);
      });
    });

    it('puts object in cache', async () => {
      const d = {
        cache: true
      };
      const key = {
        id: 'item 1',
        segment: 'unittest'
      };
      await client.set(key, d, 10000);
      const cached = await client.get(key);
      expect(cached.item).to.equal(d);
    });

    it('replaces object in cache', async () => {
      const d = {
        cache: true
      };
      const d2 = {
        update: true
      };
      const key = {
        id: 'item 1 update',
        segment: 'unittest'
      };

      await client.set(key, d, 10000);
      await client.set(key, d2, 10000);

      const data = await client.get(key);

      expect(data).to.exist();
      expect(data.item).to.exist();
      expect(data.item).to.be.an.object();
      expect(data.item.cache).to.not.exist();
      expect(data.item.update).to.equal(true);
    });

    it('throws error if circular json', async () => {
      const d = {};
      d.circular = d;

      try {
        await client.set({
          id: 'item 1',
          segment: 'unittest'
        }, d, 10000);
      } catch (err) {
        expect(err).to.be.an.error(/circular/i);
      }
    });

    it('supports empty id', async () => {
      const d = {
        cache: true
      };
      const key = {
        id: '',
        segment: 'unittest'
      };
      await client.set(key, d, 10000);

      const data = await client.get(key);
      expect(data.item).to.equal(d);
    });
  });

  describe('get()', () => {
    let client;
    before(async () => {
      client = new Client(AzureTable, options);
      await client.start();
    });

    it('without started client returns error in callback', async () => {
      const rawclient = new AzureTable(options);
      const key = {
        id: 'item 2',
        segment: 'unittest'
      };
      return rawclient.get(key).then(() => {
        fail('Should not be ok');
      }).catch((err) => {
        expect(err).to.be.an.error(/not started/);
      });
    });

    it('fetches object from cache', async () => {
      const key = {
        id: 'item 2',
        segment: 'unittest'
      };
      const d = {
        cache: true
      };
      await client.set(key, d, 10000);
      const item = await client.get(key);
      expect(item).to.exist();
    });

    it('fetches object with same data', async () => {
      const key = {
        id: 'item 3',
        segment: 'unittest'
      };
      const d = {
        cache: 'me',
        blue: false
      };
      await client.set(key, d, 10000);

      const data = await client.get(key);

      expect(data).to.exist();
      expect(data.item).to.exist();
      expect(data.item).to.be.an.object();
      expect(data.item.cache).to.equal(d.cache);
      expect(data.item.blue).to.equal(false);
    });

    it('with non-existing key id returns nothing', async () => {
      const key = {
        id: 'no-item 1',
        segment: 'unittest'
      };
      const data = await client.get(key);
      expect(data).to.not.exist();
    });

    it('with non-existing key segment returns nothing', async () => {
      const key = {
        id: 'no-item 1',
        segment: 'unittest-non-existing'
      };

      const data = await client.get(key);
      expect(data).to.not.exist();
    });

    it('with non-json data in table returns error in callback', async () => {
      const key = {
        id: 'Wrongly formatted 1',
        segment: 'unittest'
      };
      const entGen = AzureStorage.TableUtilities.entityGenerator;
      const entity = {
        PartitionKey: entGen.String(key.segment),
        RowKey: entGen.String(key.id),
        item: entGen.String('[Object weee]'),
        ttl: entGen.Int64(10)
      };

      const rawclient = client.connection.getClient();
      await rawclient.insert(entity);

      try {
        await client.get(key);
      } catch (err) {
        var getErr = err; // eslint-disable-line
      }
      expect(getErr).to.exist();
      expect(getErr.message).to.match(/Bad value content/i);
    });

    it('returns stored as timestamp', async () => {
      const key = {
        id: 'item 2 with ts',
        segment: 'unittest'
      };
      const d = {
        cache: true
      };
      await client.set(key, d, 10000);

      const data = await client.get(key);
      expect(data.stored).to.not.be.instanceOf(Date);
    });

    it('returns ttl as number', async () => {
      const key = {
        id: 'item 2 with ts',
        segment: 'unittest'
      };
      const d = {
        cache: true
      };
      await client.set(key, d, 10000);
      const data = await client.get(key);

      expect(data.ttl).to.be.a.number();
    });
  });

  describe('drop()', () => {
    let client;
    before(async () => {
      client = new Client(AzureTable, options);
      await client.start();
    });

    it('drops object from cache', async () => {
      const key = {
        id: 'item 4',
        segment: 'unittest'
      };
      const d = {
        cache: true
      };

      await client.set(key, d, 10000);
      await client.drop(key);

      const data = await client.get(key);
      expect(data).to.be.null();
    });

    it('errors on drop when using invalid key', async () => {
      await expect(client.drop({})).to.reject();
    });

    it('errors on drop when using null key', async () => {
      await expect(client.drop(null)).to.reject();
    });

    it('no errors with non-existing key id', async () => {
      const key = {
        id: 'no-item 2',
        segment: 'unittest'
      };
      await client.drop(key);
    });

    it('no error with non-existing segment', async () => {
      const key = {
        id: 'no-item 2',
        segment: 'unittest-non-existing'
      };
      await client.drop(key);
    });

    it('without started client throws', async () => {
      const rawclient = new AzureTable(options);
      const key = {
        id: 'item 4',
        segment: 'unittest'
      };

      try {
        await rawclient.drop(key);
      } catch (err) {
        var dropErr = err; // eslint-disable-line
      }
      expect(dropErr).to.exist();
    });
  });
});
