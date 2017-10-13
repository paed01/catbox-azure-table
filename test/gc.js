'use strict';

const AzureTable = require('..');
const ck = require('chronokinesis');
const Lab = require('lab');
const MockAzure = require('./MockAzure');
const TableClient = require('../lib/TableClient');
const {Client} = require('catbox');
const {Gc} = AzureTable;

const lab = exports.lab = Lab.script();
const {after, beforeEach, before, describe, it} = lab;
const {expect} = Lab.assertions;

const options = {
  connection: process.env.AZURE_TABLE_CONN,
  partition: 'unittestcachegc',
  ttl_interval: 10000
};

const {connection, partition, ttl_interval} = options;

describe('AzureTable GC', () => {
  let client;
  const timers = [];

  before(async () => {
    client = new AzureTable(options);
    await client.start();
  });
  after(async () => {
    client.stop();
    timers.forEach((t) => {
      t.unref();
      clearTimeout(t);
    });
    await TableClient(connection, partition).deleteTable();
  });
  beforeEach(async () => {
    ck.reset();
  });

  describe('start()', () => {
    it('starts timer', (done) => {
      const gc = new Gc(client.getClient(), ttl_interval);
      timers.push(gc.start());
      expect(gc.isReady()).to.be.true();
      done();
    });

    it('once', (done) => {
      const gc = new Gc(client.getClient(), ttl_interval);

      const timer = gc.start();
      timers.push(gc.start());

      expect(gc.isReady()).to.be.true();

      expect(gc.start()).to.equal(timer);

      expect(gc.isReady()).to.be.true();
      done();
    });

    it('returns timer', (done) => {
      const gc = new Gc(client.getClient(), ttl_interval);
      const timer = gc.start();
      timers.push(timer);
      expect(timer).to.be.an.object();
      done();
    });
  });

  describe('stop()', () => {
    it('stops timer', (done) => {
      const gc = new Gc(client.getClient(), ttl_interval);
      timers.push(gc.start());
      gc.stop();
      expect(gc.isReady()).to.be.false();
      done();
    });

    it('does nothing if not started', (done) => {
      const gc = new Gc(client.getClient(), ttl_interval);
      gc.stop();
      expect(gc.isReady()).to.be.false();
      done();
    });
  });

  describe('collect()', () => {
    it('deletes expired items', async () => {
      const gc = client.gc;
      const segment = 'ttltest1';

      function set(id) {
        return client.set({
          id,
          segment
        }, {
          cacheme: true
        }, 100, true);
      }

      const itemIds = ['1', '2', '3'];

      await Promise.all(itemIds.map(set));

      ck.travel(Date.now() + 200);

      const nItems = await gc.collect();
      expect(nItems).to.equal(3);

      const item = await client.getClient().get(segment, '3');
      expect(item).to.be.null();
    });

    it('ignores items that should not be collected -> gc == false', async () => {
      const gc = client.gc;
      const segment = 'ttltest2';

      function set(id) {
        return client.getClient().set(segment, id, {
          cacheme: true
        }, 50, false);
      }

      const itemIds = ['4', '5'];

      await Promise.all(itemIds.map(set));

      ck.travel(Date.now() + 200);

      await gc.collect();

      const item = await client.getClient().get(segment, '4');
      expect(item).to.exist();
    });

    it('returns 0 if no items to collect', async () => {
      const gc = new Gc(client.getClient(), ttl_interval);
      const collected = await gc.collect();
      expect(collected).to.equal(0);
    });
  });

  describe('Events', () => {
    it('emits collected event when Gc has collected', (done) => {
      const quickGcClient = new Client(AzureTable, {
        connection,
        partition,
        ttl_interval: 50
      });
      const gc = quickGcClient.connection.gc;
      gc.once('collected', () => {
        client.stop();
        done();
      });
      quickGcClient.start().catch(done);
    });

    it('emits collected event at least 2 times', (done) => {
      const quickGcClient = new Client(AzureTable, {
        connection,
        partition,
        ttl_interval: 50
      });
      const gc = quickGcClient.connection.gc;

      let count = 0;
      try {
        gc.on('collected', function EH() {
          ++count;
          if (count > 1) {
            quickGcClient.stop();
            gc.removeListener('collected', EH);
            done();
          }
        });

        quickGcClient.start().catch(done);
      } catch (err) {
        done(err);
      }
    });
  });
});

describe('Errors', () => {
  let Azure;
  before(async () => {
    Azure = MockAzure('gcerrorstest');
  });
  after(async () => {
    Azure.reset();
  });

  it('emits evict-error event if Gc failed with StorageError', (done) => {
    const quickGcClient = new Client(AzureTable, {
      connection: Azure.connectionString,
      partition: Azure.tableName,
      ttl_interval: 50
    });

    Azure.connection();
    Azure.queryFailed(500);
    quickGcClient.start();

    quickGcClient.connection.gc.once('evict-error', (err) => {
      quickGcClient.stop();

      expect(err).to.be.an.error();
      expect(err.name).to.equal('StorageError');
      expect(err.statusCode).to.equal(500);

      done();
    });

  });

  it('throws if not StorageError', (done) => {
    Azure.connection();
    Azure.queryEntities();

    const gc = Gc(TableClient(Azure.connectionString, Azure.tableName), 200);
    gc.once('collected', (n) => {
      n.a.b.c = 1;
    });
    gc.collect().catch((err) => {
      expect(err).to.be.an.error(TypeError);
      done();
    });
  });

});
