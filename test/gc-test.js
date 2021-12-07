'use strict';

const {Client} = require('@hapi/catbox');
const AzureTable = require('..');
const ck = require('chronokinesis');
const Lab = require('@hapi/lab');
const nock = require('nock');
const TableClient = require('../lib/TableClient');

const {Gc} = AzureTable;

const lab = exports.lab = Lab.script();
const {after, before, beforeEach, describe, it} = lab;

const connection = process.env.AZURE_TABLE_CONN;
const options = {
  connection,
  partition: 'unittestcachegc',
  ttl_interval: 10000,
  allowInsecureConnection: true,
};

const {partition, ttl_interval} = options;

describe('AzureTable GC', () => {
  let plugin;

  before(async () => {
    plugin = new AzureTable(options);
    await plugin.start();
  });
  after(async () => {
    await plugin.stop();
    await new TableClient(connection, partition, null, {allowInsecureConnection: true}).deleteTable();
  });
  beforeEach(async () => {
    ck.reset();
  });

  describe('init', () => {
    it('throws if ttl_interval is not a number', () => {
      expect(() => Gc(plugin.getClient())).to.throw(TypeError);
      expect(() => Gc(plugin.getClient(), 'string')).to.throw(TypeError);
      expect(() => Gc(plugin.getClient(), {})).to.throw(TypeError);
    });
  });

  describe('start()', () => {
    it('starts timer', (flags) => {
      const gc = new Gc(plugin.getClient(), ttl_interval);
      const timer = gc.start();

      flags.onCleanup = () => {
        timer.unref();
        clearTimeout(timer);
      };

      expect(gc.isReady()).to.be.true();
    });

    it('once', (flags) => {
      const gc = new Gc(plugin.getClient(), ttl_interval);

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
      const gc = new Gc(plugin.getClient(), ttl_interval);
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
      const gc = new Gc(plugin.getClient(), ttl_interval);
      const timer = gc.start();
      flags.onCleanup = () => {
        timer.unref();
        clearTimeout(timer);
      };

      gc.stop();
      expect(gc.isReady()).to.be.false();
    });

    it('does nothing if not started', () => {
      const gc = new Gc(plugin.getClient(), ttl_interval);
      expect(gc.isReady()).to.be.false();
      gc.stop();
      expect(gc.isReady()).to.be.false();
    });
  });

  describe('collect()', () => {
    it('deletes expired items', async () => {
      const gc = plugin.gc;
      const segment = 'ttltest1';

      function set(id) {
        return plugin.set({
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

      const item = await plugin.getClient().get(segment, '3');
      expect(item).to.be.null();
    });

    it('takes 100 items at a time', async () => {
      const gc = plugin.gc;
      const segment = 'ttltest2';
      const now = Date.now();

      await plugin.getClient().client.submitTransaction(new Array(100).fill().map((_, idx) => {
        return ['create', {
          partitionKey: segment,
          rowKey: 'p' + idx, 
          ttl: 10000,
          ttl_int: now + 10000,
          gc: true,
          item: '{}',
        }];
      }));

      await plugin.getClient().client.submitTransaction(new Array(50).fill().map((_, idx) => {
        return ['create', {
          partitionKey: segment,
          rowKey: 'q' + idx, 
          ttl: 60000,
          ttl_int: now + 60000,
          gc: true,
          item: '{}',
        }];
      }));

      ck.travel(Date.now() + 11000);

      const nItems1 = await gc.collect();
      expect(nItems1).to.equal(100);

      const item1 = await plugin.getClient().get(segment, 'p3');
      expect(item1).to.be.null();

      const item2 = await plugin.getClient().get(segment, 'q3');
      expect(item2).to.not.be.null();

      ck.travel(Date.now() + 61000);

      const nItems2 = await gc.collect();
      expect(nItems2).to.equal(50);

      const item3 = await plugin.getClient().get(segment, 'q3');
      expect(item3).to.be.null();
    });

    it('takes 100 items across segments', async () => {
      const gc = plugin.gc;
      const now = Date.now();

      await plugin.getClient().client.submitTransaction(new Array(51).fill().map((_, idx) => {
        return ['create', {
          partitionKey: 'ttltest4',
          rowKey: 'r' + idx, 
          ttl: 10000,
          ttl_int: now + 10000,
          gc: true,
          item: '{}',
        }];
      }));

      await plugin.getClient().client.submitTransaction(new Array(51).fill().map((_, idx) => {
        return ['create', {
          partitionKey: 'ttltest5',
          rowKey: 's' + idx, 
          ttl: 10000,
          ttl_int: now + 10000,
          gc: true,
          item: '{}',
        }];
      }));

      ck.travel(Date.now() + 11000);

      const nItems1 = await gc.collect();
      expect(nItems1).to.equal(100);

      const nItems2 = await gc.collect();
      expect(nItems2).to.equal(2);
    });

    it('ignores items that should not be collected -> gc == false', async () => {
      const gc = plugin.gc;
      const segment = 'ttltest2';

      function set(id) {
        return plugin.getClient().set(segment, id, {
          cacheme: true
        }, 50, false);
      }

      const itemIds = ['4', '5'];

      await Promise.all(itemIds.map(set));

      ck.travel(Date.now() + 200);

      await gc.collect();

      const item = await plugin.getClient().get(segment, '4');
      expect(item).to.exist();
    });

    it('returns 0 if no items to collect', async () => {
      const gc = new Gc(plugin.getClient(), ttl_interval);
      const collected = await gc.collect();
      expect(collected).to.equal(0);
    });
  });
});

describe('Stop', () => {
  it('stops timer', async () => {
    const client = new Client(AzureTable, {
      ...options,
      ttl_interval: 41,
    });
    const gc = client.connection.gc;

    let stopped = false;
    gc.on('collected', () => {
      if (stopped) throw new Error('should NOT have been stopped');
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
  beforeEach(nock.cleanAll);
  after(nock.cleanAll);

  it('emits collected event when Gc has collected', async () => {
    const client = new Client(AzureTable, {
      ...options,
      ttl_interval: 51
    });
    const gc = client.connection.gc;

    client.start();

    const n = await new Promise((resolve, reject) => {
      gc.once('collected', resolve);
      gc.once('error', reject);
    });

    client.stop();
    expect(n).to.be.a.number();
  });

  it('emits collected event at least 2 times', async () => {
    const client = new Client(AzureTable, {
      ...options,
      ttl_interval: 52
    });
    const gc = client.connection.gc;

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

    expect(n1).to.be.a.number();
    expect(n2).to.be.a.number();
  });

  it('emits error if Gc failed with RestError', async () => {
    const client = new Client(AzureTable, {
      ...options,
      ttl_interval: 53
    });
    const gc = client.connection.gc;

    client.start();

    nock(client.connection.getClient().client.url)
      .get(`/${options.partition}()`)
      .query(true)
      .reply(() => {
        return [400];
      });

    await expect(new Promise((resolve, reject) => {
      gc.once('collected', resolve);
      gc.once('error', reject);
    })).to.reject();

    client.stop();
  });

  it('emits error and stops if TypeError', async () => {
    const client = new Client(AzureTable, {
      ...options,
      ttl_interval: 54
    });
    const gc = client.connection.gc;

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
