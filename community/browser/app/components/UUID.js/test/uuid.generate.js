module("UUID.generate()");

UUIDTestCommon.testV4AsString(function() {
  return UUID.generate();
});

// vim: et ts=2 sw=2
