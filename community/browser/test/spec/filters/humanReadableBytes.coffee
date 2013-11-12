'use strict'

describe 'Filter: humanReadableBytes', () ->

  # load the filter's module
  beforeEach module 'neo4jApp.filters'

  # initialize a new instance of the filter before each test
  humanReadableBytes = {}
  beforeEach inject ($filter) ->
    humanReadableBytes = $filter 'humanReadableBytes'

  it 'should report number smaller than 1024 in bytes', () ->
    expect(humanReadableBytes 0).toBe ('0 B');
    expect(humanReadableBytes 1023).toBe ('1023 B');

  it 'should convert numbers in the KiB range', () ->
    expect(humanReadableBytes 1024).toBe ('1.00 KiB');
    expect(humanReadableBytes 1048575).toBe ('1024.00 KiB');

  it 'should convert numbers in the MiB range', () ->
    expect(humanReadableBytes 1048576).toBe ('1.00 MiB');
    expect(humanReadableBytes 1073741823).toBe ('1024.00 MiB');

  it 'should convert numbers in the GiB range', () ->
    expect(humanReadableBytes 1073741824).toBe ('1.00 GiB');
    expect(humanReadableBytes 1099511627775).toBe ('1024.00 GiB');

  it 'should convert numbers in the TiB range', () ->
    expect(humanReadableBytes 1099511627776).toBe ('1.00 TiB');
    expect(humanReadableBytes 1125899906842623).toBe ('1024.00 TiB');

  it 'should convert really large numbers to PiB', () ->
    expect(humanReadableBytes 1125899906842624).toBe ('1.00 PiB');
