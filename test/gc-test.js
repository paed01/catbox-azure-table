'use strict';

const AzureTable = require('..');
const ck = require('chronokinesis');
const Lab = require('lab');
const MockAzure = require('./MockAzure');
const TableClient = require('../lib/TableClient');
const {Client} = require('catbox');
const {Gc} = AzureTable;

const lab = exports.lab = Lab.script();
const {after, afterEach, before, beforeEach, describe, it} = lab;
const {expect} = Lab.assertions;

const connection = process.env.AZURE_TABLE_CONN;

describe('AzureTable GC', () => {
  const options = {
    connection,
    partition: 'unittestcachegc',
    ttl_interval: 10000
  };
  const {partition, ttl_interval} = options;

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

  describe('init', () => {
    it('throws if ttl_interval is not a number', (done) => {
      expect(() => Gc(client.getClient())).to.throw(TypeError);
      expect(() => Gc(client.getClient(), 'string')).to.throw(TypeError);
      expect(() => Gc(client.getClient(), {})).to.throw(TypeError);
      done();
    });
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
});

describe('Stop', () => {
  let Azure;
  beforeEach(async () => {
    Azure = MockAzure('unittestcachegcstop');
  });
  afterEach(async () => {
    Azure.reset();
  });

  it('stops timer', (done) => {
    Azure.connection();
    Azure.queryEntities(200, Azure.queryResponse());
    Azure.executeBatch();
    Azure.queryEntities(200, Azure.queryResponse());
    Azure.executeBatch();
    Azure.queryEntities(200, Azure.queryResponse());
    Azure.executeBatch();

    const client = new Client(AzureTable, {
      connection: Azure.connectionString,
      partition: Azure.tableName,
      ttl_interval: 41
    });
    const gc = client.connection.gc;

    let stopped = false;
    gc.on('collected', () => {
      if (stopped) throw new Error('should have been stopped');
      stopped = true;
      client.stop();
    });
    setTimeout(done, 100);

    client.start().catch(done);
  });
});

describe('Events', () => {
  let Azure;
  beforeEach(async () => {
    Azure = MockAzure('unittestcachegcevents');
    Azure.connection();
  });
  afterEach(async () => {
    Azure.reset();
  });

  it('emits collected event when Gc has collected', (done) => {
    const client = new Client(AzureTable, {
      connection: Azure.connectionString,
      partition: Azure.tableName,
      ttl_interval: 51
    });
    const gc = client.connection.gc;

    Azure.queryEntities(200, Azure.queryResponse());
    Azure.executeBatch();

    gc.once('collected', (n) => {
      client.stop();
      expect(Azure.isComplete()).to.be.true();
      expect(n).to.be.a.number();
      done();
    });
    client.start();
  });

  it('emits collected event at least 2 times', (done) => {
    const client = new Client(AzureTable, {
      connection: Azure.connectionString,
      partition: Azure.tableName,
      ttl_interval: 52
    });
    const gc = client.connection.gc;

    Azure.queryEntities(200, Azure.queryResponse());
    Azure.executeBatch();
    Azure.queryEntities(200, Azure.queryResponse());
    Azure.executeBatch();

    let count = 0;
    gc.on('collected', function EH(n) {
      expect(n).to.be.a.number();
      ++count;
      if (count > 1) {
        client.stop();
        expect(Azure.isComplete()).to.be.true();
        gc.removeListener('collected', EH);
        done();
      }
    });

    client.start();
  });

  it('emits error if Gc failed with StorageError', (done) => {
    const client = new Client(AzureTable, {
      connection: Azure.connectionString,
      partition: Azure.tableName,
      ttl_interval: 53
    });

    Azure.queryFailed(500);
    client.start();

    client.connection.gc.once('error', (err) => {
      client.stop();

      expect(err).to.be.an.error();
      expect(err.name).to.equal('StorageError');
      expect(err.statusCode).to.equal(500);

      done();
    });

  });

  it('emits error and stops if not StorageError', (done) => {
    const client = new Client(AzureTable, {
      connection: Azure.connectionString,
      partition: Azure.tableName,
      ttl_interval: 54
    });
    const gc = client.connection.gc;

    Azure.queryEntities();
    Azure.executeBatch();

    gc.once('collected', (n) => {
      n.a.b.c = 1;
    });
    gc.once('error', (err) => {
      expect(err).to.be.an.error(TypeError);
      expect(gc.isReady()).to.be.false();
      done();
    });
    gc.start();
  });
});
