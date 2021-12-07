'use strict';

const {EventEmitter} = require('events');
const {RestError} = require('@azure/data-tables');

const timerSymbol = Symbol.for('timer');
const stoppedSymbol = Symbol.for('stopped');

module.exports = class GarbageCollector extends EventEmitter {
  constructor(client, ttl_interval) {
    super();
    this.client = client;
    this.ttl_interval = ttl_interval;

    this[stoppedSymbol] = false;
    this[timerSymbol] = null;

    this.interval = this.interval.bind(this);
  }
  get stopped() {
    return this[stoppedSymbol];
  }
  get timer() {
    return this[timerSymbol];
  }
  isReady() {
    return !!this[timerSymbol];
  }
  collect() {
    return this.client.evict();
  }
  start() {
    this[stoppedSymbol] = false;
    if (this.timer) return this.timer;
    const timer = this[timerSymbol] = setTimeout(this.interval, this.ttl_interval);
    return timer;
  }
  stop() {
    this[stoppedSymbol] = true;
    const timer = this.timer;
    if (!timer) return;
    clearTimeout(timer);
    timer.unref();
    this[timerSymbol] = null;
  }
  async interval() {
    try {
      const n = await this.collect();
      this.emit('collected', n);
    } catch (err) {
      if (!(err instanceof RestError)) {
        this.emit('error', err);
        return this.stop();
      }
      this.emit('error', err);
    }

    if (this.stopped) return;

    const timer = this[timerSymbol] = setTimeout(this.interval, this.ttl_interval);

    return timer;
  }
};
