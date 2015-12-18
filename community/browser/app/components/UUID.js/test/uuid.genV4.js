module("UUID.genV4() as object");

(function() {

  test("basic object tests", function() {
    var n = 16;

    for (var i = 0; i < n; i++) {
      var uuid = UUID.genV4();
      equal(uuid.version, 4, "version number field");

      UUIDTestCommon.testObjectProperties(uuid);
    }
  });

})();

module("UUID.genV4() as string");

UUIDTestCommon.testV4AsString(function() {
  return UUID.genV4().hexString;
});

// vim: et ts=2 sw=2
