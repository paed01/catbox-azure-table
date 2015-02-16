var AzureStorage = require('azure-storage');
var Lab = require('lab');
var lab = exports.lab = Lab.script();

var options = {
	connection : process.env.AZURE_TABLE_CONN || 'UseDevelopmentStorage=true;DevelopmentStorageProxyUri=http://127.0.0.1;',
	partition : 'unittestcache'
};

var client = AzureStorage.createTableService(options.connection);

lab.experiment('AzureStorage', function() {
    lab.test('warming up', function(done) {
        client.createTableIfNotExists(options.partition, done);
    });
});
