module("UUID.overwrittenUUID");

test("UUID.overwrittenUUID preserves initialOccupant", 1, function() {
  ok(UUID.overwrittenUUID === initialOccupant, "UUID.overwrittenUUID === initialOccupant: " + initialOccupant);
});

// vim: et ts=2 sw=2
