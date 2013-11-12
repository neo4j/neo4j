/*!
 * ui.js
 * Copyright Mathias Bynens <http://mths.be/>
 * Modified by John-David Dalton <http://allyoucanleet.com/>
 * Available under MIT license <http://mths.be/mit>
 */
(function(window, document) {
  // The path to the Java timer applet, relative to the document.
  var archive = 'assets/nano.jar';

  // A cache of error messages.
  var errors = [];

  // Event handler cache.
  var eventHandlers = {};

  // Indicates whether the page has loaded.
  var isPageLoaded = false;

  // Benchmark results element ID prefix.
  var idPrefix = 'results-';

  // The element responsible for scrolling the page. Assumes that `ui.js` has
  // been loaded immediately before `</body>`.
  var scrollEl = document.body;

  // The UI namespace.
  var ui = new Benchmark.Suite();

  // An array of benchmarks created from test cases.
  ui.benchmarks = [];

  // The parsed query parameters from the location hash.
  ui.parameters = {};

  // API shortcuts.
  var join = Benchmark.join,
      filter = Benchmark.filter,
      formatNumber = Benchmark.formatNumber;

  // A map of operations to the CSS class names of their respective target
  // elements.
  var classNames = {
    // Used for error styles.
    'error': 'error',
    // Used to make content visible.
    'show': 'show',
    // Used to reset result styles.
    'results': 'results'
  };

  // Various application messages.
  var messages = {
    // "Run" button states.
    'run': {
      'again': 'Run again',
      'ready': 'Run tests',
      'running': 'Stop running'
    },
    // Common status values.
    'status': {
      'again': 'Done. Ready to run again.',
      'ready': 'Ready to run.'
    }
  };

  // `Benchmark.Suite#run` options.
  var runOptions = {
    'async': true,
    'queued': true
  };

  function onCycle() {
    var size = this.stats.sample.length;
    if (!this.aborted) {
      setStatus(this.name + ' &times; ' + formatNumber(this.count) + ' (' + size + ' sample' + (size === 1 ? '' : 's') + ')');
    }
  }

  function onButtonClick() {
    var isStopped = !ui.running;
    ui.abort();
    ui.length = 0;
    if (!isStopped) {
      return;
    }
    logError({ 'clear': true });
    ui.push.apply(ui, _.filter(ui.benchmarks, function(bench) {
      return !bench.error && bench.reset();
    }));
    ui.run(runOptions);
  }

  function onTitleClick(event) {
    var id;
    // Retrieve the closest benchmark results element.
    for (var target = event.target || event.srcElement; target && !(id = target.id); target = target.parentNode);
    var index = id && --id.split('-', 2)[1] || 0,
        bench = ui.benchmarks[index];
    // Queue the benchmark for running.
    ui.push(bench.reset());
    if (ui.running) {
      ui.render(index);
    } else {
      ui.run(runOptions);
    }
  }

  function onTitleKey(event) {
    var keyCode = event['which' in event ? 'which' : 'charCode' in event ? 'charCode' : 'keyCode'];
    if (keyCode == 13) {
      eventHandlers.title.click(event);
    }
  }

  function onHashChange() {
    ui.parseHash();
    if (!isPageLoaded) {
      return;
    }
    var parameters = ui.parameters,
        filterBy = parameters.filterby,
        scrollTop;

    if (filterBy) {
      scrollTop = $('results').offsetTop;
    }

    // Automatically run benchmarks.
    if ('run' in parameters) {
      scrollTop = $('runner').offsetTop;
      setTimeout(eventHandlers.button.run, 1);
    }

    // Scroll to the relevant section.
    if (scrollTop) {
      scrollEl.scrollTop = scrollTop;
    }
  }

  function onLoad() {
    addClass('controls', classNames.show);
    addListener('run', 'click', eventHandlers.button.run);

    setHTML('run', messages.run.ready);
    setHTML('user-agent', Benchmark.platform);
    setStatus(messages.status.ready);

    try {
      // Show warning when Firebug is enabled, ignoring Firebug Lite.
      // Firebug 1.9 no longer implements `console.firebug`.
      if (console.firebug || /firebug/i.test(console.table())) {
        addClass('firebug', classNames.show);
      }
    } catch (exception) {}

    // Parse the location hash. Deferred to ensure that all other `load`
    // event handlers have executed.
    setTimeout(function() {
      isPageLoaded = true;
      eventHandlers.window.hashChange();
    }, 1);
  }

  function $(element) {
    return typeof element == 'string' ? document.getElementById(element) : element;
  }

  var crlfTab = /[\r\n\t]/g;

  function addClass(element, className) {
    if (!(element = $(element))) {
      return element;
    }
    var targetClass = (' ' + element.className + ' ').replace(crlfTab, ' ');
    if (targetClass.indexOf(className) < 0) {
      element.className = (targetClass + className).trim();
    }
    return element;
  }

  function addListener(element, type, handler) {
    if (!(element = $(element))) {
      return element;
    }
    if (typeof element.addEventListener != 'undefined') {
      element.addEventListener(type, handler, false);
    } else if (typeof element.attachEvent != 'undefined') {
      element.attachEvent('on' + type, handler);
    }
    return element;
  }

  function appendHTML(element, html) {
    if ((element = $(element)) && html != null) {
      element.innerHTML += html;
    }
    return element;
  }

  function createElement(tagName) {
    return document.createElement(tagName);
  }

  function setHTML(element, html) {
    if ((element = $(element))) {
      element.innerHTML = html == null ? '' : html;
    }
    return element;
  }

  function getHz(bench) {
    return 1 / (bench.stats.mean + bench.stats.moe);
  }

  function logError(error) {
    var options = {},
        element = $('error-info');

    // Juggle arguments.
    if (typeof error == 'object' && error) {
      options = error;
      error = options.text;
    } else if (arguments.length) {
      options.text = error;
    }

    if (!element) {
      var table = $('test-table');
      element = createElement('div');
      element.id = 'error-info';
      table.parentNode.insertBefore(element, table.nextSibling);
    }

    if (options.clear === true) {
      element.className = element.innerHTML = '';
      errors.length = 0;
    }

    if ('text' in options && !_.contains(errors, error)) {
      errors.push(error);
      addClass(element, classNames.show);
      appendHTML(element, error);
    }
  }

  function setStatus(status) {
    return setHTML('status', status);
  }

  var parseHash = ui.parseHash = function parseHash() {
    var hashes = location.hash.slice(1).split('&'),
        parameters = this.parameters || (this.parameters = {});

    // Clear original parameters.
    _.forOwn(parameters, function(value, property) {
      delete parameters[property];
    });

    // Add new parameters.
    _.forEach(hashes[0], function(member) {
      var position = member.indexOf('='),
          value = '',
          parameter;

      if (member && position) {
        if (position < 0) {
          parameter = member;
        } else {
          parameter = decodeURIComponent(member.slice(0, position));
          // Parameters without corresponding values are set to `''`.
          if ((value = member.slice(position + 1))) {
            value = decodeURIComponent(value).toLowerCase();
          }
        }
        parameter = parameter.toLowerCase();
        // The values of identical parameters (e.g., `a=1&a=2`) are aggregated
        // into an array of values.
        if (parameter in parameters) {
          if (!parameters[parameter] || !_.isArray(parameters[parameter])) {
            parameters[parameter] = [parameters[parameter]];
          }
          parameters[parameter].push(value);
        } else {
          parameters[parameter] = value;
        }
      }
    });
    return this;
  };

  var render = ui.render = function render(index) {
    var iterable;
    if (index == null) {
      index = 0;
      iterable = ui.benchmarks;
    } else {
      iterable = [ui.benchmarks[index]];
    }

    _.forEach(iterable, function(bench) {
      var cell = $(idPrefix + (++index));
      // Reset the element title and class name.
      cell.title = '';
      cell.className = classNames.results;

      var error = bench.error;
      if (error) {
        // Status: Error.
        setHTML(cell, 'Error');
        addClass(cell, classNames.error);
        var serialized = join(error, '</li><li>');
        logError('<p>' + error + '.</p>' + (serialized ? '<ul><li>' + serialized + '</li></ul>' : ''));
      } else {
        // Status: Running.
        if (bench.running) {
          setHTML(cell, 'running&hellip;');
        } else if (bench.cycles) {
          // Obscure details until the suite has completed.
          if (ui.running) {
            setHTML(cell, 'completed');
          } else {
            var hz = bench.hz;
            cell.title = 'Ran ' + formatNumber(bench.count) + ' times in ' + bench.times.cycle.toFixed(3) + ' seconds.';
            setHTML(cell, formatNumber(hz.toFixed(hz < 100 ? 2 : 0)) + ' <small>&plusmn;' + bench.stats.rme.toFixed(2) + '%</small>');
          }
        } else if (ui.running && _.indexOf(ui, bench) > -1) {
          // Status: Pending.
          setHTML(cell, 'pending&hellip;');
        } else {
          // Status: Ready.
          setHTML(cell, 'ready');
        }
      }
    });
    return ui;
  };

  ui.on('add', function(event) {
    var bench = event.target,
        index = ui.benchmarks.length,
        id = index + 1,
        title = $('title-' + id);

    delete ui[ui.length];
    ui.length = 0;
    ui.benchmarks.push(bench);

    title.tabIndex = 0;
    title.title = 'Click to run this test again.';

    addListener(title, 'click', eventHandlers.title.click);
    addListener(title, 'keyup', eventHandlers.title.keyup);

    bench.on('start cycle', eventHandlers.benchmark.cycle);
    ui.render(index);
  });

  ui.on('start cycle', function() {
    ui.render();
    setHTML('run', messages.run.running);
  });

  ui.on('complete', function() {
    var benches = filter(ui.benchmarks, 'successful'),
        fastest = filter(benches, 'fastest'),
        slowest = filter(benches, 'slowest');

    ui.render();
    setHTML('run', messages.run.again);
    setStatus(messages.status.again);

    _.forEach(benches, function(bench) {
      var cell = $(idPrefix + (_.indexOf(ui.benchmarks, bench) + 1)),
          fastestHz = getHz(fastest[0]),
          hz = getHz(bench),
          percent = (1 - (hz / fastestHz)) * 100,
          span = cell.getElementsByTagName('span')[0],
          text = 'fastest';

      if (_.contains(fastest, bench)) {
        // Indicate the fastest result.
        addClass(cell, text);
      } else {
        text = isFinite(hz) ? formatNumber(percent < 1 ? percent.toFixed(2) : Math.round(percent)) + '% slower' : '';
        if (_.contains(slowest, bench)) {
          // Indicate the slowest result.
          addClass(cell, 'slowest');
        }
      }
      // Print ranking.
      if (span) {
        setHTML(span, text);
      } else {
        appendHTML(cell, '<span>' + text + '</span>');
      }
    });
  });

  eventHandlers.benchmark = {
    'cycle': onCycle
  };

  eventHandlers.button = {
    'run': onButtonClick
  };

  eventHandlers.title = {
    'click': onTitleClick,
    'keyup': onTitleKey
  };

  eventHandlers.window = {
    'hashChange': onHashChange,
    'load': onLoad
  };

  // Expose.
  window.ui = ui;

  // Parse the new query parameters when the hash changes.
  addListener(window, 'hashchange', eventHandlers.window.hashChange);

  addListener(window, 'load', eventHandlers.window.load);

  // Immediately parse the location hash.
  ui.parseHash();

  // Detect the scroll element.
  (function() {
    var scrollTop,
        div = document.createElement('div'),
        body = document.body,
        bodyStyle = body.style,
        bodyHeight = bodyStyle.height,
        html = document.documentElement,
        htmlStyle = html.style,
        htmlHeight = htmlStyle.height;

    bodyStyle.height = htmlStyle.height = 'auto';
    div.style.cssText = 'display:block;height:9001px;';
    body.insertBefore(div, body.firstChild);
    scrollTop = html.scrollTop;

    if (html.clientWidth !== 0 && ++html.scrollTop && html.scrollTop == scrollTop + 1) {
      // Set the correct `scrollEl` used by the `hashchange` event handler.
      scrollEl = html;
    }

    body.removeChild(div);
    bodyStyle.height = bodyHeight;
    htmlStyle.height = htmlHeight;
    html.scrollTop = scrollTop;
  }());

  // Inject the `nano.jar` applet.
  (function() {
    if ('nojava' in ui.parameters) {
      return addClass('java', classNames.show);
    }
    for (var begin = new Date(), measured; !(measured = new Date() - begin););
    var getNow;
    if (measured != 1 && !((getNow = window.performance) && typeof (getNow.now || getNow.webkitNow) == 'function')) {
      // Inject the applet using `innerHTML` to avoid triggering alerts in some
      // versions of IE 6.
      document.body.insertBefore(setHTML(createElement('div'), '<applet code=nano archive=' + archive + '>').lastChild, document.body.firstChild);
    }
  }());

  window.onerror = function(message, fileName, lineNumber) {
    logError('<p>' + message + '.</p><ul><li>' + join({
      'message': message,
      'fileName': fileName,
      'lineNumber': lineNumber
    }, '</li><li>') + '</li></ul>');
    scrollEl.scrollTop = $('error-info').offsetTop;
  };
}(this, document));