'use strict';

const Hoek = require('hoek');
const {EventEmitter} = require('events');

module.exports = function GarbageCollector(client, options) {
  Hoek.assert(typeof options === 'object', 'Must provide configuation to the garbage collect module.');

  const {ttl_interval} = options;

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
    if (timer) return;
    interval();
  }

  function stop() {
    if (!timer) return;
    timer.unref();
    clearTimeout(timer);
    timer = null;
  }

  function isReady() {
    return !!timer;
  }

  function interval() {
    timer = setTimeout(() => {
      collect().then(interval);
    }, ttl_interval);
  }

  function collect() {
    return client.evict().then((n) => {
      emit('collected', n);
      return n;
    }).catch((err) => {
      emit('evict-error', err);
    });
  }
};
