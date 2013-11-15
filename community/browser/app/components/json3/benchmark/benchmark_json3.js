var ui;
;(function(window) {
  // Convenience aliases.
  var getClass = {}.toString;

  // Detect asynchronous module loaders (RequireJS, `curl.js`, etc).
  var isLoader = typeof define == 'function' && typeof define.amd == 'object' && !!define.amd;

  // Detect CommonJS implementations and Node.
  var isModule = typeof require == 'function' && typeof exports == 'object' && exports != null && !isLoader;
  var isNode = isModule && typeof process == 'object' && typeof process.binding == 'function';

  // Detect web browsers.
  var isBrowser = 'window' in window && window.window == window && typeof window.document != 'undefined' && typeof window.navigator != 'undefined';

  // Detect JavaScript engines (SpiderMonkey, JS, V8, etc) and Mozilla Rhino.
  var isEngine = !isBrowser && !isModule && typeof window.load == 'function';
  var isRhino = isEngine && typeof environment != 'undefined' && environment && getClass.call(environment) == '[object Environment]';

  // Loads a module with the given `exports` name and `path`.
  function load(exports, path) {
    if (window[exports]) {
      return window[exports];
    }
    if (isModule) {
      return require(path);
    }
    if (isEngine) {
      // Normalize module paths.
      window.load(path.replace(/\.js$/, '') + '.js');
      return window[exports];
    }
    return null;
  }

  // Synchronously loads a library with the given `exports` name and `path`.
  function getLibrary(exports, path) {
    // Normalize paths.
    path = path.replace(/\.js$/, '') + '.js';
    if (isNode) {
      // Use Node's `vm` module.
      var vm = require('vm'), fs = require('fs');
      if (!fs.existsSync(path)) {
        throw Error('The script `' + path + '` does not exist.');
      }
      var scope = {};
      scope[exports] = null;
      var context = vm.createContext(scope);
      vm.runInNewContext(fs.readFileSync(path, 'utf8'), context);
      return context[exports];
    }
    if (isRhino) {
      // Use Rhino's reflection mechanisms via LiveConnect.
      var io = java.io, javascript = Packages.org.mozilla.javascript, Context = javascript.Context;
      try {
        var context = Context.enter();
        // Evaluate all scripts in interpretive mode (avoids overflow errors).
        context.setOptimizationLevel(-1);
        // Set the JS version to 1.5 for compatibility with ECMA-262.
        context.setLanguageVersion(150);
        // Create the constructors for the standard built-in ECMAScript objects.
        var scope = context.initStandardObjects();
        var script = (new java.io.File(path)).getAbsoluteFile();
        if (!script.isFile()) {
          throw Error('The script `' + path + '` does not exist.');
        }
        scope[exports] = null;
        context.evaluateReader(scope, new io.BufferedReader(new io.FileReader(script)), script.getName(), 1, null);
      } finally {
        // Leave the execution context, even if an error occurred.
        Context.exit();
      }
      return scope[exports];
    }
    var previous, result;
    if (isBrowser) {
      var transport = typeof ActiveXObject != 'undefined' ? new ActiveXObject('Microsoft.XMLHTTP') : typeof XMLHttpRequest != 'undefined' ? new XMLHttpRequest() : null;
      // Retrieve the script contents using a synchronous `XMLHttpRequest`.
      transport.open('GET', path + (path.indexOf('?') < 0 ? '?' : '&') + '_=' + (+new Date()), false);
      transport.send(null);
      // Catch script errors.
      var onError = window.onerror, exception;
      window.onerror = function(message) {
        exception = message;
        return true;
      };
      // Remove the current exports.
      previous = window[exports];
      window[exports] = null;
      // Inject the script into the document to force evaluation.
      var script = document.createElement('script'), firstScript = document.scripts.item(0);
      // Append a `sourceURL` to facilitate debugging.
      script.text = transport.responseText + '\n/*\n//@ sourceURL=' + path + '\n*/';
      firstScript.parentNode.insertBefore(script, firstScript).parentNode.removeChild(script);
      // Capture the exported value and restore the previous exports and
      // `onerror` event handler.
      result = window[exports];
      window[exports] = previous;
      window.onerror = onError;
      // Re-throw caught exceptions.
      if (exception) {
        throw Error(exception);
      }
      return result;
    }
    if (isEngine) {
      // Simple overwrite-load-restore for other engines.
      previous = window[exports];
      window[exports] = null;
      window.load(path);
      result = window[exports];
      window[exports] = previous;
      return result;
    }
  }

  // Logs the given `value`.
  function log(value) {
    if (typeof console != 'undefined' && console && typeof console.log != 'undefined') {
      return console.log.apply(console, arguments);
    }
    if (typeof print == 'function' && !isBrowser) {
      // In browsers, the global `print` function shows the "Print" dialog.
      return print(value);
    }
    throw value;
  }

  // Load Benchmark.
  var Benchmark = load('Benchmark', '../vendor/benchmark'), _ = load('_', '../vendor/lodash'), platform = load('platform', '../vendor/platform');
  if (!Benchmark.Suite) {
    Benchmark = Benchmark.runInContext({
      '_': _,
      'platform': platform
    });
  }

  // JSON 3.
  var JSON3 = getLibrary('JSON', '../lib/json3');

  // JSON 2.
  var JSON2 = getLibrary('JSON', '../vendor/json2');

  // `json-parse-state` (Crockford's state machine parser).
  var json_parse_state = getLibrary('json_parse', '../vendor/json_parse_state');

  // `json-parse` (Crockford's recursive descent parser).
  var json_parse = getLibrary('json_parse', '../vendor/json_parse');

  // Mike Samuel's `json-sans-eval`.
  var json_sans_eval = getLibrary('jsonParse', '../vendor/json_sans_eval');

  // Asen Bozhilov's `evalJSON`
  var evalJSON = getLibrary('evalJSON', '../vendor/evalJSON');

  var suite = typeof ui == 'object' && ui || new Benchmark.Suite('JSON 3 Benchmark Suite');

  var value = '{"kitcambridge":"Kit","contributors":{"jdalton":"John-David","mathias":"Mathias"},"list":[1,2,3],"number":5,"date":"2012-04-25T14:08:36.879Z","boolean":true,"nil":null}';

  suite.add('JSON 3: `parse`', function() {
    JSON3.parse(value);
  });

  suite.add('JSON 2: `parse`', function() {
    JSON2.parse(value);
  });

  suite.add('`json-parse-state`', function() {
    json_parse_state(value);
  });

  suite.add('`json-parse`', function() {
    json_parse(value);
  });

  suite.add('`json-sans-eval`', function() {
    json_sans_eval(value);
  });

  suite.add('`evalJSON`', function() {
    evalJSON(value);
  });

  if (!isBrowser) {
    suite.on('cycle', function(event) {
      log(String(event.target));
    }).on('complete', function() {
      var results = this.filter("successful"), fastest = results.filter("fastest"), slowest = results.filter("slowest");
      _.forEach(results, function(result) {
        var hz = result.hz;
        if (_.contains(fastest, result)) {
          log("Fastest: `%s`.", result.name);
        } else if (_.contains(slowest, result)) {
          log("Slowest: `%s`.%s", result.name, isFinite(hz) ? " " + Math.round((1 - hz / fastest[0].hz) * 100) + "% slower." : "");
        }
      });
    });

    suite.run({
      'async': true
    });
  }
}(this));