/*!
 * HTML5.js v1.0.0-rc
 * Copyright 2012 John-David Dalton <http://allyoucanleet.com/>
 * Based on HTML5 Shiv vpre3.3 | @afarkas @jon_neal @rem | MIT/GPL2 Licensed
 * Available under MIT/GPL2 license
 */
;(function(window, document) {
  'use strict';

  /** Preset for the install/uninstall methods */
  var allOptions = { 'methods': true, 'print': true, 'styles': true };

  /** Cache of created elements, document methods, and install state */
  var html5Cache = {};

  /** Previous `html5` object */
  var old = window.html5;

  /** List of HTML5 node names to install support for */
  var nodeNames = [
    'abbr', 'article', 'aside', 'audio', 'bdi', 'canvas', 'data', 'datalist',
    'details', 'figcaption', 'figure', 'footer', 'header', 'main',
    'mark', 'meter', 'nav', 'output', 'progress', 'section', 'summary', 'time',
    'video'
  ];

  /** Used to namespace printable elements and define `expando` */
  var namespace = 'html5js';

  /** Used to store an elements `uid` if `element.uniqueNumber` is not supported */
  var expando = namespace + /\d+$/.exec(Math.random());

  /** Used to filter media types */
  var reMedia = /^$|\b(?:all|print)\b/;

  /**
   * Used to skip elements with type attributes because in IE they cannot be
   * set/changed once an element is inserted into a document/fragment.
   * http://msdn.microsoft.com/en-us/library/ie/ms534700(v=vs.85).aspx
   */
  var reSkip = /^(?:button|select)$/i;

  /** Used to detect elements that cannot be cloned correctly */
  var reUnclonable = /^<\?/;

  /** Used as a fallback for `element.uniqueNumber` */
  var uid = 1;

  /** Cache of unclonable element node names */
  var unclonables = {};

  /**
   * An object used to flag features.
   *
   * @static
   * @memberOf html5
   * @type Object
   */
  var support = {};

  (function() {
    var p,
        parent,
        sandbox;

    // create a new document used to get untainted styles
    try {
      // avoid https: protocol issues with IE
      sandbox = new ActiveXObject(location.protocol == 'https:' && 'htmlfile');
    } catch(e) {
      // http://xkr.us/articles/dom/iframe-document/
      (sandbox = document.createElement('iframe')).name = expando;
      sandbox.frameBorder = sandbox.height = sandbox.width = 0;
      parent = document.body || document.documentElement;
      parent.insertBefore(sandbox, parent.firstChild);
      sandbox = (sandbox = sandbox.contentWindow || sandbox.contentDocument || frames[expando]).document || sandbox;
    }
    sandbox.write('<!doctype html><title></title><body><script>document.w = this<\/script>');
    sandbox.close();

    p = sandbox.body.appendChild(sandbox.createElement('p'));
    p.innerHTML = '<nav/>';

    /**
     * Detect whether the browser supports default HTML5 styles.
     * @memberOf html5.support
     * @type Boolean
     */
    support.html5Styles = !!p.firstChild &&
      (p.firstChild.currentStyle || sandbox.w.getComputedStyle(p.firstChild, null)).display == 'block';

    /**
     * Detect whether the browser supports unknown elements.
     *
     * @memberOf html5.support
     * @type Boolean
     */
    support.unknownElements = p.childNodes.length == 1 || (function() {
      // assign a false positive if unable to install
      try {
        (document.createElement)('p');
      } catch(e) {
        return true;
      }
      var frag = document.createDocumentFragment();
      return (
        typeof frag.createElement == 'undefined' ||
        typeof p.uniqueNumber == 'undefined'
      );
    }());

    /**
     * Detect whether the browser supports printing html5 elements.
     *
     * @memberOf html5.support
     * @type Boolean
     */
    support.html5Printing = support.unknownElements || (
      // assign a false positive if unable to install
      typeof document.namespaces == 'undefined' ||
      typeof document.parentWindow == 'undefined' ||
      typeof p.applyElement == 'undefined' ||
      typeof p.removeNode == 'undefined' ||
      typeof window.attachEvent == 'undefined'
    );

    parent && destroyElement(sandbox.w.frameElement);
  }());

  /*--------------------------------------------------------------------------*/

  /**
   * Creates a style sheet of modified CSS rules to style the print wrappers.
   * (e.g. the CSS rule "header{}" becomes "html5js\:header{}")
   *
   * @private
   * @param {Document} ownerDocument The document.
   * @param {String} cssText The CSS text.
   * @returns {StyleSheet} The style element.
   */
  function addPrintSheet(ownerDocument, cssText) {
    var pair,
        parts = cssText.split('{'),
        index = parts.length,
        reElements = RegExp('(^|[\\s,>+~])(' + nodeNames.join('|') + ')(?=[[\\s,>+~#.:]|$)', 'gi'),
        replacement = '$1' + namespace + '\\:$2';

    while (index--) {
      pair = parts[index] = parts[index].split('}');
      pair[pair.length - 1] = pair[pair.length - 1].replace(reElements, replacement);
      parts[index] = pair.join('}');
    }
    return addStyleSheet(ownerDocument, parts.join('{'));
  }

  /**
   * Wraps all HTML5 elements in the given document with printable elements.
   * (e.g. the "header" element is wrapped with the "html5js:header" element)
   *
   * @private
   * @param {Document} ownerDocument The document.
   * @returns {Array} An array of added wrappers.
   */
  function addPrintWrappers(ownerDocument) {
    var node,
        nodes = ownerDocument.getElementsByTagName('*'),
        index = nodes.length,
        reElements = RegExp('^(?:' + nodeNames.join('|') + ')$', 'i'),
        result = [];

    while (index--) {
      node = nodes[index];
      if (reElements.test(node.nodeName)) {
        result.push(node.applyElement(createPrintWrapper(node)));
      }
    }
    return result;
  }

  /**
   * Creates a style sheet with the given CSS text and adds it to the document.
   *
   * @private
   * @param {Document} ownerDocument The document.
   * @param {String} cssText The CSS text.
   * @returns {StyleSheet} The style sheet.
   */
  function addStyleSheet(ownerDocument, cssText) {
    // IE8 only respects namespace prefixs when created with `innerHTML`
    var p = ownerDocument.createElement('p'),
        parent = ownerDocument.getElementsByTagName('head')[0] || ownerDocument.documentElement;

    p.innerHTML = 'x<style>' + cssText + '</style>';
    return parent.insertBefore(p.lastChild, parent.firstChild);
  }

  /**
   * Creates HTML5 elements using the given document enabling the document to
   * parse them correctly.
   *
   * @private
   * @param {Document|Fragment} ownerDocument The document.
   * @returns {Document|Fragment} The document.
   */
  function createElements(ownerDocument) {
    var index = nodeNames.length,
        create = ownerDocument.createElement;

    while (index--) {
      create(nodeNames[index]);
    }
    return ownerDocument;
  }

  /**
   * Creates a printable wrapper for the given element.
   *
   * @private
   * @param {Element} element The element.
   * @returns {Element} The wrapper.
   */
  function createPrintWrapper(element) {
    var node,
        nodes = element.attributes,
        index = nodes.length,
        wrapper = element.ownerDocument.createElement(namespace + ':' + element.nodeName);

    // copy element attributes to the wrapper
    while (index--) {
      node = nodes[index];
      node.specified && wrapper.setAttribute(node.nodeName, node.nodeValue);
    }
    // copy element styles to the wrapper
    wrapper.style.cssText = element.style.cssText;
    return wrapper;
  }

  /**
   * Destroys the given element.
   *
   * @private
   * @param {Element} element The element to destroy.
   * @param {Object} [cache] The cache object.
   */
  function destroyElement(element, cache) {
    var trash = (cache || getCache(element.ownerDocument)).trash;
    trash.appendChild(element);
    trash.innerHTML = '';
  }

  /**
   * Gets the cache object for the given document.
   *
   * @private
   * @param {Document} ownerDocument The document.
   * @returns {Object} The cache object.
   */
  function getCache(ownerDocument) {
    var docEl = ownerDocument.documentElement,
        id = docEl.uniqueNumber || docEl[expando] || (docEl[expando] = uid++),
        skip = support.unknownElements;

    return html5Cache[id] || (html5Cache[id] = {
      'frag': !skip && createElements(ownerDocument.createDocumentFragment()),
      'nativeCreateElement': !skip && createElements(ownerDocument).createElement,
      'nativeCreateFragment': !skip && ownerDocument.createDocumentFragment,
      'nodes': {},
      'trash': ownerDocument.createElement('div')
    });
  }

  /**
   * Removes the given print wrappers, leaving the original elements.
   *
   * @private
   * @param {Document} ownerDocument The document.
   * @params {Array} wrappers An array of wrappers.
   */
  function removePrintWrappers(ownerDocument, wrappers) {
    var cache = getCache(ownerDocument),
        index = wrappers.length;

    while (index--) {
      destroyElement(wrappers[index].removeNode(), cache);
    }
  }

  /**
   * Resolves an options object from the given value.
   *
   * @private
   * @param {Mixed} value The value to convert to an options object.
   * @returns {Object} The options object.
   */
  function resolveOptions(value) {
    var key;
    value = value ? (value === 'all' || value.all ? allOptions : value) : {};

    if (typeof value == 'string') {
      var object = {};
      value = value.split(/[, ]+/);
      while ((key = value.pop())) {
        object[key] = true;
      }
      value = object;
    }
    return value;
  }

  /**
   * Overwrites the document's `createElement` and `createDocumentFragment` methods
   * with `html5.createElement` and `html5.createDocumentFragment` equivalents.
   *
   * @private
   * @param {Document} ownerDocument The document.
   */
  function setMethods(ownerDocument) {
    var cache = getCache(ownerDocument),
        create = cache.nativeCreateElement,
        frag = cache.frag,
        nodes = cache.nodes;

    // allow a small amount of repeated code for better performance
    ownerDocument.createElement = function(nodeName) {
      var cached = nodes[nodeName],
          node = cached ? cached.cloneNode() : create(nodeName);

      if (!cached && !unclonables[nodeName] &&
          !(unclonables[nodeName] = reUnclonable.test(node.outerHTML))) {
        node = (nodes[nodeName] = node).cloneNode();
      }
      return node.canHaveChildren && !reSkip.test(nodeName) ? frag.appendChild(node) : node;
    };

    // compile unrolled `createElement` calls for better performance
    ownerDocument.createDocumentFragment = Function('frag',
      'return function() {\n' +
      '  var node = frag.cloneNode(), create = node.createElement;\n' +
      (nodeNames + '').replace(/\w+/g, 'create("$&")\n') + ';\n' +
      '  return node\n' +
      '}'
    )(frag);
  }

  /**
   * Adds support for printing HTML5 elements.
   *
   * @private
   * @param {Document} ownerDocument The document.
   */
  function setPrintSupport(ownerDocument) {
    var printSheet,
        wrappers,
        cache = getCache(ownerDocument),
        namespaces = ownerDocument.namespaces,
        ownerWindow = ownerDocument.parentWindow;

    ownerWindow.attachEvent('onbeforeprint', cache.onbeforeprint = function() {
      var imports,
          length,
          sheet,
          collection = ownerDocument.styleSheets,
          cssText = [],
          index = collection.length,
          sheets = [];

      // convert styleSheets collection to an array
      while (index--) {
        sheets[index] = collection[index];
      }
      // concat all style sheet CSS text
      while ((sheet = sheets.pop())) {
        // IE does not enforce a same origin policy for external style sheets...
        if (!sheet.disabled && reMedia.test(sheet.media)) {
          // ...but will throw an "access denied" error when attempting to read
          // the CSS text of a style sheet added by a script from a different origin.
          try {
            cssText.push(sheet.cssText);
            for (imports = sheet.imports, index = 0, length = imports.length; index < length; index++) {
              sheets.push(imports[index]);
            }
          } catch(e) { }
        }
      }
      // wrap all HTML5 elements with printable elements and add print style sheet
      wrappers = addPrintWrappers(ownerDocument);
      printSheet = addPrintSheet(ownerDocument, cssText.reverse().join(''));
    });

    ownerWindow.attachEvent('onafterprint', cache.onafterprint = function() {
      // remove wrappers, leaving the original elements, and remove print style sheet
      removePrintWrappers(ownerDocument, wrappers);
      destroyElement(printSheet, cache);
    });

    if (typeof namespaces[namespace] == 'undefined') {
      namespaces.add(namespace);
    }
  }

  /**
   * Adds minimal default HTML5 element styles to the given document.
   *
   * @private
   * @param {Document} ownerDocument The document.
   * @param {Object} options Options object.
   */
  function setStyles(ownerDocument, options) {
    // for additional default and normalized HTML5 element styles checkout
    // https://github.com/necolas/normalize.css
    getCache(ownerDocument).sheet = addStyleSheet(ownerDocument,
      // corrects block display not defined in IE6/7/8/9 and Firefox 3
      'article, aside, figcaption, figure, footer, header, main, nav, section {' +
      '  display: block' +
      '}' +
      // adds styling not present in IE6/7/8/9
      'mark {' +
      '  background: #ff0;' +
      '  color: #000' +
      '}'
    );
  }

  /**
   * Restores the document's original `createElement` and `createDocumentFragment` methods.
   *
   * @private
   * @param {Document} ownerDocument The document.
   */
  function unsetMethods(ownerDocument) {
    var cache = getCache(ownerDocument),
        fn = cache.nativeCreateElement;

    if (ownerDocument.createElement != fn) {
      ownerDocument.createElement = fn;
    }
    if (ownerDocument.createDocumentFragment != (fn = cache.nativeCreateFragment)) {
      ownerDocument.createDocumentFragment = fn;
    }
  }

  /**
   * Removes support for printing HTML5 elements.
   *
   * @private
   * @param {Document} ownerDocument The document.
   */
  function unsetPrintSupport(ownerDocument) {
    var cache = getCache(ownerDocument),
        ownerWindow = ownerDocument.parentWindow;

    ownerWindow.detachEvent('onbeforeprint', cache.onbeforeprint || unsetPrintSupport);
    ownerWindow.detachEvent('onafterprint', cache.onafterprint || unsetPrintSupport);
  }

  /**
   * Removes default HTML5 element styles.
   *
   * @private
   * @param {Document} ownerDocument The document.
   * @param {Object} options Options object.
   */
  function unsetStyles(ownerDocument, options) {
    var cache = getCache(ownerDocument),
        sheet = cache.sheet;

    if (sheet) {
      cache.sheet = null;
      destroyElement(sheet, cache);
    }
  }

  /*--------------------------------------------------------------------------*/

  /**
   * Creates a shimmed element of the given node name.
   *
   * @memberOf html5
   * @param {Document} [ownerDocument=document] The context document.
   * @param {String} nodeName The node name of the element to create.
   * @returns {Element} The created element.
   * @example
   *
   * // basic usage
   * html5.createElement('div');
   *
   * // from a child iframe
   * parent.html5.createElement(document, 'div');
   */
  function createElement(ownerDocument, nodeName) {
    // juggle arguments
    ownerDocument || (ownerDocument = document);
    if (ownerDocument && !ownerDocument.nodeType) {
      nodeName = ownerDocument;
      ownerDocument = document;
    }
    if (support.unknownElements) {
      return ownerDocument.createElement(nodeName);
    }
    // Avoid adding some elements to fragments in IE because
    // * attributes like `type` cannot be set/changed once an element is inserted
    //   into a document/fragment
    // * link elements with `src` attributes that are inaccessible, as with
    //   a 403 response, will cause the tab/window to crash
    // * script elements appended to fragments will execute when their `src`
    //   or `text` property is set
    var cache = getCache(ownerDocument),
        nodes = cache.nodes,
        cached = nodes[nodeName],
        node = cached ? cached.cloneNode() : cache.nativeCreateElement(nodeName);

    // IE < 9 doesn't clone unknown elements correctly
    if (!cached && !unclonables[nodeName] &&
        !(unclonables[nodeName] = reUnclonable.test(node.outerHTML))) {
      node = (nodes[nodeName] = node).cloneNode();
    }
    return node.canHaveChildren && !reSkip.test(nodeName) ? cache.frag.appendChild(node) : node;
  }

  /**
   * Creates a shimmed document fragment.
   *
   * @memberOf html5
   * @param {Document} [ownerDocument=document] The context document.
   * @returns {Fragment} The created document fragment.
   * @example
   *
   * // basic usage
   * html5.createDocumentFragment();
   *
   * // from a child iframe
   * parent.html5.createDocumentFragment(document);
   */
  function createDocumentFragment(ownerDocument) {
    ownerDocument || (ownerDocument = document);
    return support.unknownElements
      ? ownerDocument.createDocumentFragment()
      : createElements(getCache(ownerDocument).frag.cloneNode());
  }

  /**
   * Installs shims according to the specified options.
   *
   * @memberOf html5
   * @param {Document} [ownerDocument=document] The document.
   * @param {Object} [options={}] Options object.
   * @returns {Document} The document.
   * @example
   *
   * // basic usage
   * // autmatically called on the primary document to allow IE < 9 to
   * // parse HTML5 elements correctly
   * html5.install();
   *
   * // from a child iframe
   * parent.html5.install(document);
   *
   * // with an options object
   * html5.install({
   *
   *   // overwrite the document's `createElement` and `createDocumentFragment`
   *   // methods with `html5.createElement` and `html5.createDocumentFragment` equivalents.
   *   'methods': true,
   *
   *   // add support for printing HTML5 elements
   *   'print': true,
   *
   *   // add minimal default HTML5 element styles
   *   'styles': true
   * });
   *
   * // with an options string
   * html5.install('print styles');
   *
   * // from a child iframe with options
   * parent.html5.install(document, options);
   *
   * // using a shortcut to install all support extensions
   * html5.install('all');
   */
  function install(ownerDocument, options) {
    ownerDocument || (ownerDocument = document);
    if (ownerDocument && !ownerDocument.nodeType) {
      options = ownerDocument;
      ownerDocument = document;
    }

    options = resolveOptions(options);
    uninstall(ownerDocument, {
      'methods': options.methods,
      'print': options.print,
      'styles': options.styles
    });

    if (!support.html5Styles && options.styles) {
      setStyles(ownerDocument, options);
    }
    if (!support.html5Printing && options.print) {
      setPrintSupport(ownerDocument);
    }
    if (!support.unknownElements) {
      // if not installing methods then init cache and install support
      // for basic HTML5 element parsing
      options.methods ? setMethods(ownerDocument) : getCache(ownerDocument);
    }
    return ownerDocument;
  }

  /**
   * Restores a previously overwritten `html5` object.
   * @memberOf html5
   * @returns {Object} The current `html5` object.
   */
  function noConflict() {
    window.html5 = old;
    return this;
  }

  /**
   * Uninstalls shims according to the specified options.
   *
   * @memberOf html5
   * @param {Document} [ownerDocument=document] The document.
   * @param {Object} [options={}] Options object.
   * @returns {Document} The document.
   * @example
   *
   * // basic usage with an options object
   * html5.uninstall({
   *
   *   // restore the document's original `createElement`
   *   // and `createDocumentFragment` methods.
   *   'methods': true,
   *
   *   // remove support for printing HTML5 elements
   *   'print': true,
   *
   *   // remove minimal default HTML5 element styles
   *   'styles': true
   * });
   *
   * // with an options string
   * html5.uninstall('print styles');
   *
   * // from a child iframe with options
   * parent.html5.uninstall(document, options);
   *
   * // using a shortcut to uninstall all support extensions
   * html5.uninstall('all');
   */
  function uninstall(ownerDocument, options) {
    ownerDocument || (ownerDocument = document);
    if (ownerDocument && !ownerDocument.nodeType) {
      options = ownerDocument;
      ownerDocument = document;
    }
    options = resolveOptions(options);
    if (!support.unknownElements && options.methods) {
      unsetMethods(ownerDocument);
    }
    if (!support.html5Printing && options.print) {
      unsetPrintSupport(ownerDocument);
    }
    if (!support.html5Styles && options.styles) {
      unsetStyles(ownerDocument, options);
    }
    return ownerDocument;
  }

  /*--------------------------------------------------------------------------*/

  /**
   * The `html5` object.
   * @type Object
   */
  var html5 = {

    /**
     * The semantic version number.
     * @static
     * @memberOf html5
     * @type String
     */
    'version': '1.0.0-rc',

    // an object of feature detection flags
    'support': support,

    // creates shimmed document fragments
    'createDocumentFragment': createDocumentFragment,

    // creates shimmed elements
    'createElement': createElement,

    // installs support extensions
    'install': install,

    // avoid `html5` object conflicts
    'noConflict': noConflict,

    // uninstalls support extensions
    'uninstall': uninstall
  };

  /*--------------------------------------------------------------------------*/

  // Expose the `html5` object to the global object even when an AMD loader is
  // present in case html5.js was injected by a third-party script and not
  // intended to be loaded as a module. The global assignment can be reverted in
  // the `html5` module via its `noConflict()` method.
  window.html5 = html5;

  // some AMD build optimizers, like r.js, check for specific condition patterns like the following:
  if (typeof define == 'function' && typeof define.amd == 'object' && define.amd) {
    // define as an anonymous module so, through path mapping, it can be aliased
    define(function() {
      return html5;
    });
  }
}(this, document));