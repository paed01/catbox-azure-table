'use strict';

const AzureTable = require('..');
const ck = require('chronokinesis');
const Lab = require('@hapi/lab');
const MockAzure = require('./MockAzure');
const TableClient = require('../lib/TableClient');
const {Client} = require('@hapi/catbox');
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

  before(async () => {
    client = new AzureTable(options);
    await client.start();
  });
  after(async () => {
    client.stop();
    await TableClient(connection, partition).deleteTable();
  });
  beforeEach(async () => {
    ck.reset();
  });

  describe('init', () => {
    it('throws if ttl_interval is not a number', () => {
      expect(() => Gc(client.getClient())).to.throw(TypeError);
      expect(() => Gc(client.getClient(), 'string')).to.throw(TypeError);
      expect(() => Gc(client.getClient(), {})).to.throw(TypeError);
    });
  });

  describe('start()', () => {
    it('starts timer', (flags) => {
      const gc = new Gc(client.getClient(), ttl_interval);
      const timer = gc.start();

      flags.onCleanup = () => {
        timer.unref();
        clearTimeout(timer);
      };

      expect(gc.isReady()).to.be.true();
    });

    it('once', (flags) => {
      const gc = new Gc(client.getClient(), ttl_interval);

      const timer = gc.start();
      flags.onCleanup = () => {
        timer.unref();
        clearTimeout(timer);
      };

      expect(gc.isReady()).to.be.true();
      expect(gc.start()).to.equal(timer);
      expect(gc.isReady()).to.be.true();
    });

    it('returns timer', (flags) => {
      const gc = new Gc(client.getClient(), ttl_interval);
      const timer = gc.start();
      expect(timer).to.be.an.object();

      flags.onCleanup = () => {
        timer.unref();
        clearTimeout(timer);
      };
    });
  });

  describe('stop()', () => {
    it('stops timer', (flags) => {
      const gc = new Gc(client.getClient(), ttl_interval);
      const timer = gc.start();
      flags.onCleanup = () => {
        timer.unref();
        clearTimeout(timer);
      };

      gc.stop();
      expect(gc.isReady()).to.be.false();
    });

    it('does nothing if not started', () => {
      const gc = new Gc(client.getClient(), ttl_interval);
      expect(gc.isReady()).to.be.false();
      gc.stop();
      expect(gc.isReady()).to.be.false();
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

  it('stops timer', async () => {
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

    client.start();
    return new Promise((resolve) => {
      setTimeout(resolve, 100);
    });
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

  it('emits collected event when Gc has collected', async () => {
    const client = new Client(AzureTable, {
      connection: Azure.connectionString,
      partition: Azure.tableName,
      ttl_interval: 51
    });
    const gc = client.connection.gc;

    Azure.queryEntities(200, Azure.queryResponse());
    Azure.executeBatch();

    client.start();

    const n = await new Promise((resolve, reject) => {
      gc.once('collected', resolve);
      gc.once('error', reject);
    });

    client.stop();
    expect(Azure.isComplete()).to.be.true();
    expect(n).to.be.a.number();
  });

  it('emits collected event at least 2 times', async () => {
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

    client.start();

    const n1 = await new Promise((resolve, reject) => {
      gc.once('collected', resolve);
      gc.once('error', reject);
    });
    const n2 = await new Promise((resolve, reject) => {
      gc.once('collected', resolve);
      gc.once('error', reject);
    });

    client.stop();

    expect(Azure.isComplete()).to.be.true();
    expect(n1).to.be.a.number();
    expect(n2).to.be.a.number();
  });

  it('emits error if Gc failed with StorageError', async () => {
    const client = new Client(AzureTable, {
      connection: Azure.connectionString,
      partition: Azure.tableName,
      ttl_interval: 53
    });
    const gc = client.connection.gc;

    Azure.queryFailed(500);
    client.start();

    await expect(new Promise((resolve, reject) => {
      gc.once('collected', resolve);
      gc.once('error', reject);
    })).to.reject();
  });

  it('emits error and stops if not StorageError', async () => {
    const client = new Client(AzureTable, {
      connection: Azure.connectionString,
      partition: Azure.tableName,
      ttl_interval: 54
    });
    const gc = client.connection.gc;

    Azure.queryEntities();
    Azure.executeBatch();

    gc.on('collected', (n) => {
      n.a.b.c = 1;
    });
    gc.start();

    await expect(new Promise((resolve, reject) => {
      gc.once('collected', resolve);
      gc.once('error', reject);
    })).to.reject(TypeError);

    expect(gc.isReady()).to.be.false();
  });
});
