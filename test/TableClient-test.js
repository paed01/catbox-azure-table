'use strict';

const TableClient = require('../lib/TableClient');
const nock = require('nock');

const {after, beforeEach, describe, expect, it} = exports.lab = require('@hapi/lab').script();

describe('TableClient', () => {
  beforeEach(nock.cleanAll);
  after(nock.cleanAll);

  describe('connect', () => {
    it('connect on connect returns same instance', async () => {
      const client = new TableClient(process.env.AZURE_TABLE_CONN, 'tableclienttest', null, {
        allowInsecureConnection: true,
        retryOptions: {
          maxRetries: 0,
        },
      });

      const connects = await Promise.all([client.connect(), client.connect()]);
      expect(connects[0] === connects[1]).to.be.true();
    });

    it('connect when already ready is ok', async () => {
      const client = new TableClient(process.env.AZURE_TABLE_CONN, 'tableclienttest', null, {
        allowInsecureConnection: true,
        retryOptions: {
          maxRetries: 0,
        },
      });

      await client.connect();
      await client.connect();
    });
  });

  describe('drop', () => {
    it('non 404 error throws', async () => {
      const client = new TableClient(process.env.AZURE_TABLE_CONN, 'tableclienttest', null, {
        allowInsecureConnection: true,
        retryOptions: {
          maxRetries: 0,
        },
      });

      await client.connect();

      nock(client.client.url)
        .delete(/.*/)
        .query(true)
        .reply(() => {
          return [400];
        });

      try {
        await client.drop('pkey', 'dropper');
      } catch (e) {
        var err = e;
      }

      expect(err).to.be.instanceof(Error);
    });
  });
});
