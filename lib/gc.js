'use strict';

const Hoek = require('hoek');
const {EventEmitter} = require('events');

module.exports = function GarbageCollector(client, ttl_interval) {
  Hoek.assert(typeof ttl_interval === 'number', 'Must provide ttl_interval, interval in milliseconds to collect items');

  let timer;

  const api = Object.assign(new EventEmitter(), {
    collect,
    isReady,
    start,
    stop,
  });

  function emit(...args) {
    api.emit(...args);
  }

  return api;

  function start() {
    if (timer) return timer;
    return interval();
  }

  function stop() {
    if (!timer) return;
    clearTimeout(timer);
    timer.unref();
    timer = null;
  }

  function isReady() {
    return !!timer;
  }

  function interval() {
    timer = setTimeout(() => {
      collect().then(interval);
    }, ttl_interval);
    return timer;
  }

  function collect() {
    return client.evict().then((n) => {
      emit('collected', n);
      return n;
    }, (err) => {
      if (err.name !== 'StorageError') {
        stop();
      }
      emit('evict-error', err);
    });
  }
};
