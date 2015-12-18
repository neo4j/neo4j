var verbose = true;
var nTrials = 1 << 12;
var put = function(message) { document.writeln(message); };

var v1 = /^[0-9a-f]{8}-[0-9a-f]{4}-1[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f][13579bdf][0-9a-f]{10}$/;
var v4 = /^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/;

var nErrors = 0;

if (typeof UUID.generate == "function") {
  put("Testing UUID.generate() ...")
  var table = {};
  for (var i = 0; i < nTrials; i++) {
    var uuid = UUID.generate();
    if (!v4.test(uuid)) {
      nErrors++;
      put("Invalid UUIDv4 format\t" + uuid);
    } else if (table[uuid]) {
      nErrors++;
      put("Duplicate UUID was generated\t" + uuid);
    } else {
      table[uuid] = true;
      if (verbose) {
        put("OK\t" + uuid);
      }
    }
  }
  put("Finished UUID.generate() test.");
}

if (typeof UUID.genV4 == "function") {
  put("Testing UUID.genV4() ...")
  var table = {};
  for (var i = 0; i < nTrials; i++) {
    var uuid = UUID.genV4().toString();
    if (!v4.test(uuid)) {
      nErrors++;
      put("Invalid UUIDv4 format\t" + uuid);
    } else if (table[uuid]) {
      nErrors++;
      put("Duplicate UUID was generated\t" + uuid);
    } else {
      table[uuid] = true;
      if (verbose) {
        put("OK\t" + uuid);
      }
    }
  }
  put("Finished UUID.genV4() test.");
}

if (typeof UUID.genV1 == "function") {
  put("Testing UUID.genV1() ...")
  var table = {};
  for (var i = 0; i < nTrials; i++) {
    var uuid = UUID.genV1().toString();
    if (!v1.test(uuid)) {
      nErrors++;
      put("Invalid UUIDv1 format\t" + uuid);
    } else if (table[uuid]) {
      nErrors++;
      put("Duplicate UUID was generated\t" + uuid);
    } else {
      table[uuid] = true;
      if (verbose) {
        put("OK\t" + uuid);
      }
    }
  }
  put("Finished UUID.genV1() test.");
}

put(nErrors + " error(s) occurred.");

// vim: et ts=2 sw=2
