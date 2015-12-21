'use strict';

const AzureStorage = require('azure-storage');
const Lab = require('lab');
const lab = exports.lab = Lab.script();

const connection = process.env.AZURE_TABLE_CONN || 'UseDevelopmentStorage=true;DevelopmentStorageProxyUri=http://127.0.0.1;';

lab.experiment('warming up', () => {
  const client = AzureStorage.createTableService(connection);
  lab.test('regular tests', (done) => {
    client.createTableIfNotExists('unittestcache', done);
  });
  lab.test('gc tests', (done) => {
    client.createTableIfNotExists('unittestcachegc', done);
  });
});
