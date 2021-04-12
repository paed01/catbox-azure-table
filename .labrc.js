'use strict';

const {expect} = require('@hapi/code');
global.expect = expect;

module.exports = {
  assert: '@hapi/code',
  timeout: 2000,
  verbose: true,
  globals: 'expect',
};
