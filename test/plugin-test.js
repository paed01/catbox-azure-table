'use strict';

const CatboxAzureTable = require('..');
const TableClient = require('../lib/TableClient');
const { TableClient: AzureTableClient } = require('@azure/data-tables');
const { Client } = require('@hapi/catbox');
const { expect } = require('@hapi/code');

const { after, before, describe, fail, it } = exports.lab = require('@hapi/lab').script();

const options = {
  connection: process.env.AZURE_TABLE_CONN,
  partition: 'unittestcache',
  ttl_interval: false,
  allowInsecureConnection: true,
};

const { connection, partition } = options;

describe('AzureTable plugin', () => {
  after(() => {
    return new TableClient(connection, partition, null, { allowInsecureConnection: true }).deleteTable();
  });

  describe('options', () => {
    it('options.client as AzureTableClient sets table client', () => {
      const azureClient = AzureTableClient.fromConnectionString(options.connection, options.partition, { allowInsecureConnection: true });
      const client = new Client(CatboxAzureTable, {
        client: azureClient,
        partition,
        ttl_interval: false,
      });

      expect(client.connection.getClient().client === azureClient).to.be.true();
    });

    it('options.client that is not an AzureTableClient throws', () => {
      expect(() => {
        new Client(CatboxAzureTable, {
          client: {},
          partition,
          ttl_interval: 111,
        });
      }).to.throw(TypeError, /instance of azure TableClient/);
    });

    it('options connection url and credential works', async () => {
      const azureClient = AzureTableClient.fromConnectionString(options.connection, options.partition, { allowInsecureConnection: true });

      const client = new Client(CatboxAzureTable, {
        ...options,
        connection: azureClient.url,
        credential: azureClient.credential,
      });

      await client.connection.getClient().connect();
      await client.set({ id: 'withurl', segment: 'plugin' });

      expect(new URL(client.connection.getClient().client.url).pathname).to.equal("/devstoreaccount1");
    });

    it('options without connection defaults to development storage', () => {
      const client = new Client(CatboxAzureTable, {
        partition,
        ttl_interval: false
      });

      expect(new URL(client.connection.getClient().client.url).pathname).to.equal("/devstoreaccount1");
    });

    it('instantiate without no options complains about ttl_interval', () => {
      function fn() {
        new Client(CatboxAzureTable);
      }

      expect(fn).to.throw(Error, /ttl_interval/);
    });

    it('instantiate with partitition only complains about ttl_interval', () => {
      function fn() {
        new Client(CatboxAzureTable, {
          partition: 'catbox'
        });
      }

      expect(fn).to.throw(Error, /ttl_interval/);
    });

    it('instantiate with ttl_interval = true throws an error', () => {
      function fn() {
        new Client(CatboxAzureTable, {
          partition: 'catbox',
          ttl_interval: true
        });
      }

      expect(fn).to.throw(Error, /ttl_interval/);
    });

    it('instantiate with ttl_interval that is not a number throws', () => {
      function fn() {
        new Client(CatboxAzureTable, {
          partition: 'catbox',
          ttl_interval: 'string'
        });
      }

      expect(fn).to.throw(Error, /ttl_interval/);
    });

    it('instantiate with negative ttl_interval throws', () => {
      function fn() {
        new Client(CatboxAzureTable, {
          partition: 'catbox',
          ttl_interval: -1
        });
      }

      expect(fn).to.throw(Error, /ttl_interval/);
    });
  });

  describe('start()', () => {
    it('returns ready connection', async () => {
      const client = new Client(CatboxAzureTable, options);
      await client.start();
      expect(client.isReady()).to.equal(true);
    });

    it('second call to start is ignored if started', async () => {
      const client = new Client(CatboxAzureTable, options);
      await client.start();
      await client.start();
      expect(client.isReady()).to.equal(true);
    });

    it('throws if partition (tableName) does not match naming convention', () => {
      try {
        new Client(CatboxAzureTable, {
          connection,
          partition: 'cache-me',
          ttl_interval: false
        });
      } catch (err) {
        expect(err).to.be.an.error(/Table name format/i);
      }
    });

    it('throws error if connection string is malformatted', async () => {
      try {
        new Client(CatboxAzureTable, {
          connection: 'somewhere',
          partition,
          ttl_interval: false
        });
      } catch (err) {
        expect(err).to.be.an.error();
      }
    });
  });

  describe('interface', () => {
    it('set without starting returns error', async () => {
      const client = new Client(CatboxAzureTable, options);
      try {
        await client.set({
          id: 'item 2',
          segment: 'unittest'
        }, {
          cacheme: true
        }, Infinity);
        
        fail('Should not be ok');
      } catch (err) {
        expect(err).to.match(/Disconnected/i);
      }
    });

    it('get without starting returns error', async () => {
      const client = new Client(CatboxAzureTable, options);
      try {
        await client.get({
          id: '1',
          segment: '2'
        });
        
        fail('Should not be ok');
      } catch (err) {
        expect(err).to.match(/Disconnected/i);
      }
    });

    it('drop without started client throws', async () => {
      const client = new Client(CatboxAzureTable, options);
      try {
        await client.drop({
          id: 'item 4',
          segment: 'unittest',
        });
        fail('Should not be ok');
      } catch (err) {
        expect(err).to.match(/Disconnected/i);
      }
    });

    it('get non-existing item returns null', async () => {
      const client = new Client(CatboxAzureTable, options);
      await client.start();
      const data = await client.get({
        id: 'non-existing',
        segment: '2'
      });
      expect(data).to.equal(null);
    });

    it('drop item twice returns null', async () => {
      const client = new CatboxAzureTable(options);
      await client.start();

      await client.set({
        id: 'to-drop',
        segment: '2'
      }, {
        cacheme: true
      });

      const firstDrop = await client.drop({
        id: 'to-drop',
        segment: '2'
      });

      expect(firstDrop).to.exist();

      const secondDrop = await client.drop({
        id: 'to-drop',
        segment: '2'
      });

      expect(secondDrop).to.equal(null);
    });

    it('drop item with invalid id returns error', async () => {
      const client = new CatboxAzureTable(options);
      await client.start();

      client.drop({
        id: 'two\rlines',
        segment: '2'
      }).then(() => {
        fail('Should not be ok');
      }).catch((err) => {
        expect(err).to.be.an.error();
      });
    });

    it('supports empty id', async () => {
      const client = new CatboxAzureTable(options);
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
      const client = new Client(CatboxAzureTable, options);
      await client.start();
      client.stop();
      client.stop();
      expect(client.isReady()).to.be.false();
    });

    it('can be stopped even if not started', () => {
      const client = new Client(CatboxAzureTable, options);
      function fn() {
        client.stop();
      }

      expect(fn).to.not.throw();
    });

    it('can be started again', async () => {
      const client = new Client(CatboxAzureTable, options);
      await client.start();
      client.stop();
      await client.start();
      expect(client.isReady()).to.be.true();
    });

    it('stops Gc as well', async () => {
      const client = new Client(CatboxAzureTable, {
        ...options,
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
      const client = new Client(CatboxAzureTable, options);
      expect(client.isReady()).to.not.be.true();
    });

    it('returns true if started', async () => {
      const client = new Client(CatboxAzureTable, options);
      await client.start();
      expect(client.isReady()).to.be.true();
    });
  });

  describe('validateSegmentName()', () => {
    it('returns null if validated', () => {
      const client = new Client(CatboxAzureTable, options);
      expect(client.validateSegmentName('table')).to.be.null();
    });

    it('returns Error if empty string', () => {
      const client = new Client(CatboxAzureTable, options);
      expect(client.validateSegmentName('')).to.be.instanceOf(Error);
    });

    it('returns Error if nothing passed', () => {
      const client = new Client(CatboxAzureTable, options);
      expect(client.validateSegmentName()).to.be.instanceOf(Error);
    });

    it('returns Error if null', () => {
      const client = new Client(CatboxAzureTable, options);
      expect(client.validateSegmentName(null)).to.be.instanceOf(Error);
    });

    it('returns Error if \\0', () => {
      const client = new Client(CatboxAzureTable, options);
      expect(client.validateSegmentName('\0')).to.be.instanceOf(Error);
    });
  });

  describe('set()', () => {
    let client;

    before(async () => {
      client = new Client(CatboxAzureTable, options);
      await client.start();
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
      client = new Client(CatboxAzureTable, options);
      await client.start();
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
      const entity = {
        partitionKey: key.segment,
        rowKey: key.id,
        item: '[Object weee]',
        ttl: 10,
      };

      const rawclient = client.connection.getClient().client;
      await rawclient.upsertEntity(entity);

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
      client = new Client(CatboxAzureTable, options);
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
  });
});
