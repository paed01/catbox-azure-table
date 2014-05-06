/*jshint expr:true es5:true */

var Catbox = require('catbox');

var Lab = require('lab');
var AzureTable = require('..');

var expect = Lab.expect;
var before = Lab.before;
// var beforeEach = Lab.beforeEach;
// var after = Lab.after;
var describe = Lab.experiment;
var it = Lab.test;

var options = {
    partition : 'unittestcache'
};

describe('AzureTable', function () {
    it('throws an error if not created with new', function (done) {

        var fn = function () {
            var client = AzureTable();
        };

        expect(fn).to.throw(Error);
        done();
    });

    describe('#start', function () {
        it('returns no error', function (done) {
            var client = new Catbox.Client(AzureTable, options);
            client.start(function (err) {
                expect(err).to.not.exist;
                done();
            });
        });

        it('returns error if partition (tableName) does not match naming convention', function (done) {
            var client = new Catbox.Client(AzureTable, {
                    connection : options.connection,
                    partition : 'cache-me'
                });
            client.start(function (err) {
                expect(err).to.exist;
                done();
            });
        });
    });

    describe('#isReady', function () {
        it('returns false if not started', function (done) {
            var client = new Catbox.Client(AzureTable, options);
            expect(client.isReady()).to.not.be.ok;
            done();
        });

        it('returns true if started', function (done) {
            var client = new Catbox.Client(AzureTable, options);
            client.start(function (err) {
                expect(client.isReady()).to.be.ok;
                done();
            });
        });
    });

    describe('#set', function () {
        var client = new Catbox.Client(AzureTable, options);

        before(function (done) {
            client.start(function (err) {
                expect(err).to.not.exist;
                done();
            });
        });

        it('puts object in cache', function (done) {
            var d = {
                cache : true
            };
            client.set({
                id : 'item 1',
                segment : 'unittest'
            }, d, 10000, function (err) {
                expect(err).to.not.exist;
                done();
            });
        });

        it('replaces object in cache', function (done) {
            var d = {
                cache : true
            };
            var key = {
                id : 'item 1 update',
                segment : 'unittest'
            };

            client.set(key, d, 10000, function (err) {
                expect(err).to.not.exist;

                var d2 = {
                    update : true
                };
                client.set(key, d2, 10000, function (err) {
                    client.get(key, function (err, data) {
                        expect(err).to.not.exist;

                        expect(data).to.exist;
                        expect(data.item).to.exist;
                        expect(data.item).to.be.an('object');
                        expect(data.item.cache).to.not.exist;
                        expect(data.item.update).to.equal(true);

                        done();
                    });
                });

            });
        });

    });

    describe('#get', function () {
        var client = new Catbox.Client(AzureTable, options);
        before(function (done) {
            client.start(function (err) {
                expect(err).to.not.exist;
                done();
            });
        });

        it('fetches object from cache', function (done) {
            var key = {
                id : 'item 2',
                segment : 'unittest'
            };
            var d = {
                cache : true
            };
            client.set(key, d, 10000, function (err) {
                expect(err).to.not.exist;

                client.get(key, function (err) {
                    expect(err).to.not.exist;
                    done();
                });
            });
        });

        it('fetches object from with same data', function (done) {
            var key = {
                id : 'item 3',
                segment : 'unittest'
            };
            var d = {
                cache : 'me',
                blue : false
            };
            client.set(key, d, 10000, function (err) {
                expect(err).to.not.exist;

                client.get(key, function (err, data) {
                    expect(err).to.not.exist;

                    expect(data).to.exist;
                    expect(data.item).to.exist;
                    expect(data.item).to.be.an('object');
                    expect(data.item.cache).to.equal(d.cache);
                    expect(data.item.blue).to.equal(false);

                    done();
                });
            });
        });

        it('with non-existing key id returns nothing', function (done) {
            var key = {
                id : 'no-item 1',
                segment : 'unittest'
            };
            client.get(key, function (err, data) {
                expect(err).to.not.exist;
                expect(data).to.not.exist;
                done();
            });
        });

        it('with non-existing key segment returns nothing', function (done) {
            var key = {
                id : 'no-item 1',
                segment : 'unittest-non-existing'
            };
            client.get(key, function (err, data) {
                expect(err).to.not.exist;
                expect(data).to.not.exist;
                done();
            });
        });

    });

    describe('#drop', function () {
        var client = new Catbox.Client(AzureTable, options);
        before(function (done) {
            client.start(function (err) {
                expect(err).to.not.exist;
                done();
            });
        });

        it('drops object from cache', function (done) {
            var key = {
                id : 'item 4',
                segment : 'unittest'
            };
            var d = {
                cache : true
            };
            client.set(key, d, 10000, function (err) {
                expect(err).to.not.exist;

                client.drop(key, function (err) {
                    expect(err).to.not.exist;
                    done();
                });
            });
        });

        it('with non-existing key id returns nothing', function (done) {
            var key = {
                id : 'no-item 2',
                segment : 'unittest'
            };
            client.drop(key, function (err) {
                expect(err).to.not.exist;
                done();
            });
        });

        it('with non-existing key segment returns nothing', function (done) {
            var key = {
                id : 'no-item 2',
                segment : 'unittest-non-existing'
            };
            client.get(key, function (err) {
                expect(err).to.not.exist;
                done();
            });
        });

    });
});
