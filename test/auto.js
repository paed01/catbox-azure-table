var AzureStorage = require('azure-storage');
var Lab = require('lab');
var lab = exports.lab = Lab.script();

var connection = process.env.AZURE_TABLE_CONN || 'UseDevelopmentStorage=true;DevelopmentStorageProxyUri=http://127.0.0.1;';

lab.experiment('warming up', function() {
    var client = AzureStorage.createTableService(connection);
    lab.test('regular tests', function(done) {
        client.createTableIfNotExists('unittestcache', done);
    });
    lab.test('gc tests', function(done) {
        client.createTableIfNotExists('unittestcachegc', done);
    });
});
