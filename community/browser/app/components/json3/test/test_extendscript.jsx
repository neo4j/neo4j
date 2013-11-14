function main() {
  var exports = this.Spec = {};
  #include "../vendor/spec/lib/spec.js"

  exports = this.Newton = {};
  #include "../vendor/spec/lib/newton.js"

  exports = this.JSON = {};
  #include "../lib/json3.js"

  try {
    #include "./test_json3.js"
  } catch (exception) {
    $.bp();
    print(exception.source.split('\n')[exception.line - 1] + '\n' + exception);
  }
}

main.call({
  'load': function load(identifier, path) {
    return this[identifier];
  }
});