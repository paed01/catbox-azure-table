'use strict';

const AzureTable = require('..');
const ck = require('chronokinesis');
const Lab = require('lab');
const TableClient = require('../lib/TableClient');
const {Gc} = AzureTable;

const lab = exports.lab = Lab.script();
const {after, beforeEach, before, describe, it} = lab;
const {expect} = Lab.assertions;

const options = {
  connection: process.env.AZURE_TABLE_CONN,
  partition: 'unittestcachegc',
  ttl_interval: 10000
};

const {connection, partition} = options;

describe('AzureTable GC', () => {
  let client, settings;

  before(async () => {
    client = new AzureTable(options);
    await client.start();
    settings = Object.assign(options);
  });
  beforeEach(async () => {
    ck.reset();
  });
  after(async () => {
    await TableClient(connection, partition).deleteTable();
  });

  describe('start()', () => {
    it('starts timer', (done) => {
      const gc = new Gc(client.getClient(), settings);
      gc.start();
      expect(gc.isReady()).to.be.true();
      done();
    });

    it('once', (done) => {
      const gc = new Gc(client.getClient(), settings);
      gc.start();
      expect(gc.isReady()).to.be.true();
      gc.start();
      expect(gc.isReady()).to.be.true();
      done();
    });
  });

  describe('stop()', () => {
    it('stops timer', (done) => {
      const gc = new Gc(client.getClient(), settings);
      gc.start();
      gc.stop();
      expect(gc.isReady()).to.be.false();
      done();
    });

    it('does nothing if not started', (done) => {
      const gc = new Gc(client.getClient(), settings);
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
      const gc = new Gc(client.getClient(), settings);
      const collected = await gc.collect();
      expect(collected).to.equal(0);
    });
  });
});
