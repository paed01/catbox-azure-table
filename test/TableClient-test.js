'use strict';

const MockAzure = require('./MockAzure');
const TableClient = require('../lib/TableClient');

const {after, before, describe, expect, it} = exports.lab = require('lab').script();

describe('TableClient', () => {
  let Azure;
  before(async () => {
    Azure = MockAzure('tableclienttest');
  });
  after(async () => {
    Azure.reset();
  });

  describe('connect', () => {
    it('throws if connection failed', async () => {
      Azure.tableNotFound();
      Azure.tableBeingDeletedError();

      const client = TableClient(Azure.connectionString, Azure.tableName);
      await expect(client.connect()).to.reject(/The specified table is being deleted/i);
    });
  });

  describe('query items', () => {
    it('throws if query fails', async () => {
      Azure.connection();
      Azure.queryEntities(404);

      const client = TableClient(Azure.connectionString, Azure.tableName);
      await client.connect();

      try {
        await client.evict();
      } catch (err) {
        expect(err).to.be.an.error();
        expect(err.statusCode).to.equal(404);
      }
    });
  });

  describe('evict', () => {
    it('an empty query response is ignored', async () => {
      Azure.connection();
      Azure.queryEntities(204, null);

      const client = TableClient(Azure.connectionString, Azure.tableName);
      await client.connect();
      const n = await client.evict();

      expect(n).to.equal(0);
    });

    it('an empty query response is ignored', async () => {
      Azure.connection();

      const qresp = Azure.queryResponse();
      qresp.value = [];
      Azure.queryEntities(200, qresp);

      const client = TableClient(Azure.connectionString, Azure.tableName);
      await client.connect();
      const n = await client.evict();

      expect(n).to.equal(0);
    });

    it('batch failed throws', async () => {
      Azure.connection();
      Azure.queryEntities(200, Azure.queryResponse());
      Azure.executeBatch(500, Azure.errorResponse('StorageError', 'Malformed'));

      const client = TableClient(Azure.connectionString, Azure.tableName);
      await client.connect();
      await expect(client.evict().catch((err) => {
        throw err;
      })).to.reject('Malformed');
    });

    it('100 items are ok', async () => {
      const queryEntities = [];
      const PartitionKey = 'testlen';
      for (let i = 0; i < 100; i++) {
        queryEntities.push(Azure.createResponseEntity({PartitionKey}));
      }

      Azure.connection();
      Azure.queryEntities(200, Azure.queryResponse(queryEntities));
      Azure.executeBatch();

      const client = TableClient(Azure.connectionString, Azure.tableName);
      await client.connect();
      const n = await client.evict();
      expect(n).to.equal(100);
    });

    it('100 items per segment/PartitionKey are ok', async () => {
      const queryEntities = [];
      for (let i = 0; i < 100; i++) {
        queryEntities.push(Azure.createResponseEntity({PartitionKey: 'testlen1'}));
      }
      for (let i = 0; i < 100; i++) {
        queryEntities.push(Azure.createResponseEntity({PartitionKey: 'testlen2'}));
      }

      Azure.connection();
      Azure.queryEntities(200, Azure.queryResponse(queryEntities));
      Azure.executeBatch();
      Azure.executeBatch();

      const client = TableClient(Azure.connectionString, Azure.tableName);
      await client.connect();
      const n = await client.evict();
      expect(n).to.equal(200);

      expect(Azure.isComplete(), 'No more calls expected').to.be.true();
    });

    it('101 items are NOT ok', async () => {
      const queryEntities = [];
      const segment = 'testlen';
      for (let i = 0; i < 101; i++) {
        queryEntities.push(Azure.createResponseEntity({segment}));
      }

      Azure.connection();
      Azure.queryEntities(200, Azure.queryResponse(queryEntities));
      Azure.executeBatch();

      const client = TableClient(Azure.connectionString, Azure.tableName);
      await client.connect();

      try {
        await client.evict();
      } catch (err) {
        expect(err).to.be.an.error('Batches must not contain more than 100 operations.');
      }
    });
  });

  describe('delete table', () => {
    it('throws if delete table failed', async () => {
      Azure.connection();
      Azure.deleteTableIfExists(400, Azure.errorResponse('StorageError', 'Malformed'));

      const client = TableClient(Azure.connectionString, Azure.tableName);
      await expect(client.deleteTable().catch((err) => {
        throw err;
      })).to.reject('Malformed');
    });
  });
});
