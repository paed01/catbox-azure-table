'use strict';

const Lab = require('lab');
const nock = require('nock');
const TableClient = require('../lib/TableClient');

const lab = exports.lab = Lab.script();
const {after, before, describe, it} = lab;
const {expect} = Lab.assertions;

describe('TableClient', () => {
  before(async () => {
    nock.disableNetConnect();
  });
  after(async () => {
    nock.enableNetConnect();
  });

  describe('connect', () => {
    it('throws if connection failed', async () => {
      nock('http://catboxazuretable.table.core.windows.local')
        .get('/Tables(%27test%27)')
        .reply(503, {
          message: 'invalid'
        });

      const client = TableClient('DefaultEndpointsProtocol=http;AccountName=catboxazuretable;AccountKey=YXBwYXBwYXBw;EndpointSuffix=core.windows.local', 'test');
      try {
        await client.connect();
      } catch (err) {
        expect(err).to.be.an.error();
      }
    });
  });
});
