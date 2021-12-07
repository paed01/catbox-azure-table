'use strict';

const {expect} = require('@hapi/code');
global.expect = expect;

const nock = require('nock');
nock.enableNetConnect(/(localhost|127\.0\.0\.1):\d+/);

process.env.AZURE_TABLE_CONN = process.env.AZURE_TABLE_CONN || 'DefaultEndpointsProtocol=http;AccountName=devstoreaccount1;AccountKey=Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==;TableEndpoint=http://127.0.0.1:10002/devstoreaccount1;';

module.exports = {
  assert: '@hapi/code',
  timeout: 2000,
  verbose: true,
  leaks: false,
};
