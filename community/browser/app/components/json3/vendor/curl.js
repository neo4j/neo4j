/** @license MIT License (c) copyright B Cavalier & J Hann */

/**
 * curl (cujo resource loader)
 * An AMD-compliant javascript module and resource loader
 *
 * curl is part of the cujo.js family of libraries (http://cujojs.com/)
 *
 * Licensed under the MIT License at:
 * 		http://www.opensource.org/licenses/mit-license.php
 *
 * @version 0.6
 */
(function (global) {

	/*
	 * Basic operation:
	 * When a dependency is encountered and it already exists, it's returned.
	 * If it doesn't already exist, it is created and the dependency's script
	 * is loaded. If there is a define call in the loaded script with a id,
	 * it is resolved asap (i.e. as soon as the module's dependencies are
	 * resolved). If there is a (single) define call with no id (anonymous),
	 * the resource in the resNet is resolved after the script's onload fires.
	 * IE requires a slightly different tactic. IE marks the readyState of the
	 * currently executing script to 'interactive'. If we can find this script
	 * while a define() is being called, we can match the define() to its id.
	 * Opera marks scripts as 'interactive' but at inopportune times so we
	 * have to handle it specifically.
	 */

	var
		version = '0.6.0',
		userCfg = global['curl'],
		doc = global.document,
		head = doc && (doc['head'] || doc.getElementsByTagName('head')[0]),
		// constants / flags
		msgUsingExports = {},
		interactive = {},
		// this is the list of scripts that IE is loading. one of these will
		// be the "interactive" script. too bad IE doesn't send a readystatechange
		// event to tell us exactly which one.
		activeScripts = {},
		// these are always handy :)
		toString = ({}).toString,
		undef,
		// script ready states that signify it's loaded
		readyStates = { 'loaded': 1, 'interactive': interactive, 'complete': 1 },
		// local cache of resource definitions (lightweight promises)
		cache = {},
		// preload are files that must be loaded before any others
		preload = false,
		// net to catch anonymous define calls' arguments (non-IE browsers)
		argsNet,
		// RegExp's used later, "cached" here
		dontAddExtRx = /\?/,
		absUrlRx = /^\/|^[^:]+:\/\//,
		findLeadingDotsRx = /(?:^|\/)(\.)(\.?)\/?/g,
		removeCommentsRx = /\/\*[\s\S]*?\*\/|(?:[^\\])\/\/.*?[\n\r]/g,
		findRValueRequiresRx = /require\s*\(\s*["']([^"']+)["']\s*\)|(?:[^\\]?)(["'])/g,
		cjsGetters,
		core;

	function noop () {}

	function isType (obj, type) {
		return toString.call(obj).indexOf('[object ' + type) == 0;
	}

	function normalizePkgDescriptor (descriptor) {
		var path, main;

		path = descriptor.path = removeEndSlash(descriptor['path'] || descriptor['location'] || '');
		main = descriptor['main'] || 'main';
		descriptor.config = descriptor['config'];
		descriptor.main = main.charAt(0) == '.' ?
			removeEndSlash(reduceLeadingDots(main, path)) :
			joinPath(path, main);

		return descriptor;
	}

	function joinPath (path, file) {
		return removeEndSlash(path) + '/' + file;
	}

	function removeEndSlash (path) {
		return path && path.charAt(path.length - 1) == '/' ? path.substr(0, path.length - 1) : path;
		//return endsWithSlash(path) ? path.substr(0, path.length - 1) : path;
	}

	function reduceLeadingDots (childId, baseId) {
		// this algorithm is similar to dojo's compactPath, which interprets
		// module ids of "." and ".." as meaning "grab the module whose name is
		// the same as my folder or parent folder".  These special module ids
		// are not included in the AMD spec.
		var levels, removeLevels, isRelative;
		removeLevels = 1;
		childId = childId.replace(findLeadingDotsRx, function (m, dot, doubleDot) {
			if (doubleDot) removeLevels++;
			isRelative = true;
			return '';
		});
		// TODO: throw if removeLevels > baseId levels
		if (isRelative) {
			levels = baseId.split('/');
			levels.splice(levels.length - removeLevels, removeLevels);
			// childId || [] is a trick to not concat if no childId
			return levels.concat(childId || []).join('/');
		}
		else {
			return childId;
		}
	}

	function pluginParts (id) {
		var delPos = id.indexOf('!');
		return {
			resourceId: id.substr(delPos + 1),
			// resourceId can be zero length
			pluginId: delPos >= 0 && id.substr(0, delPos)
		};
	}

	function Begetter () {}

	function beget (parent) {
		Begetter.prototype = parent;
		var child = new Begetter();
		Begetter.prototype = undef;
		return child;
	}

	function Promise () {

		var self, thens, complete;

		self = this;
		thens = [];

		function then (resolved, rejected, progressed) {
			// capture calls to callbacks
			thens.push([resolved, rejected, progressed]);
		}

		function notify (which, arg) {
			// complete all callbacks
			var aThen, cb, i = 0;
			while ((aThen = thens[i++])) {
				cb = aThen[which];
				if (cb) cb(arg);
			}
		}

		complete = function promiseComplete (success, arg) {
			// switch over to sync then()
			then = success ?
				function (resolved, rejected) { resolved && resolved(arg); } :
				function (resolved, rejected) { rejected && rejected(arg); };
			// we no longer throw during multiple calls to resolve or reject
			// since we don't really provide useful information anyways.
			complete = noop;
			// complete all callbacks
			notify(success ? 0 : 1, arg);
		};

		this.then = function (resolved, rejected, progressed) {
			then(resolved, rejected, progressed);
			return self;
		};
		this.resolve = function (val) {
			self.resolved = val;
			complete(true, val);
		};
		this.reject = function (ex) {
			self.rejected = ex;
			complete(false, ex);
		};
		this.progress = function (msg) {
			notify(2, msg);
		}

	}

	var ResourceDef = Promise; // subclassing isn't worth the extra bytes

	function isPromise (o) {
		return o instanceof Promise;
	}

	function when (promiseOrValue, callback, errback, progback) {
		// we can't just sniff for then(). if we do, resources that have a
		// then() method will make dependencies wait!
		if (isPromise(promiseOrValue)) {
			promiseOrValue.then(callback, errback, progback);
		}
		else {
			callback(promiseOrValue);
		}
	}

	core = {

		createResourceDef: function (id, cfg, isPreload, optCtxId) {
			var def, ctxId;

			ctxId = optCtxId == undef ? id : optCtxId;

			def = new ResourceDef();
			def.id = id;
			def.isPreload = isPreload;

			// replace cache with resolved value (overwrites self in cache)
			def.then(function (res) { cache[id] = res; });

			// functions that dependencies will use:

			function toAbsId (childId) {
				return reduceLeadingDots(childId, ctxId);
			}

			function toUrl (n) {
				// even though internally, we don't seem to need to do
				// toAbsId, the AMD spec says we need to do this for plugins.
				// also, thesec states that we should not append an extension
				// in this function.
				return core.resolvePathInfo(toAbsId(n), cfg).url;
			}

			function localRequire (ids, callback) {
				var cb, rvid, childDef, earlyExport;

				// this is a public function, so remove ability for callback
				// fixes issue #41
				cb = callback && function () { callback.apply(undef, arguments[0]); };

				// RValue require (CommonJS)
				if (isType(ids, 'String')) {
					// return resource
					rvid = toAbsId(ids);
					childDef = cache[rvid];
					earlyExport = isPromise(childDef) && childDef.exports;
					if (!(rvid in cache) || (isPromise(childDef) && !earlyExport)) {
						throw new Error('Module not resolved: '  + rvid);
					}
					if (cb) {
						throw new Error('require(id, callback) not allowed');
					}
					return earlyExport || childDef;
				}

				// pass the callback, so the main def won't get resolved!
				core.getDeps(def, ids, cb);

			}

			def.require = localRequire;
			localRequire['toUrl'] = toUrl;
			def.toAbsId = toAbsId;

			return def;
		},

		getCjsRequire: function (def) {
			return def.require;
		},

		getCjsExports: function (def) {
			return def.exports || (def.exports = {});
		},

		getCjsModule: function (def) {
			var module = def.module;
			if (!module) {
				module = def.module = {
					'id': def.id,
					'uri': core.getDefUrl(def),
					'exports': core.getCjsExports(def)
				};
				module.exports = module['exports'];
			}
			return module;
		},

		getDefUrl: function (def) {
			// note: don't look up an anon module's id from it's own toUrl cuz
			// the parent's config was used to find this module
			// the toUrl fallback is for named modules in built files
			// which must have absolute ids.
			return def.url || (def.url = core.checkToAddJsExt(def.require['toUrl'](def.id)));
		},

		extractCfg: function (cfg) {
			var pluginCfgs;

			// set defaults and convert from closure-safe names
			cfg.baseUrl = cfg['baseUrl'] || '';
			cfg.pluginPath = 'pluginPath' in cfg ? cfg['pluginPath'] : 'curl/plugin';

			// create object to hold path map.
			// each plugin and package will have its own pathMap, too.
			cfg.pathMap = {};
			pluginCfgs = cfg.plugins = cfg['plugins'] || {};

			// temporary arrays of paths. this will be converted to
			// a regexp for fast path parsing.
			cfg.pathList = [];

			// normalizes path/package info and places info on either
			// the global cfg.pathMap or on a plugin-specific altCfg.pathMap.
			// also populates a pathList on cfg or plugin configs.
			function fixAndPushPaths (coll, isPkg) {
				var id, pluginId, data, parts, pluginCfg, info;
				for (var name in coll) {
					data = coll[name];
					pluginCfg = cfg;
					// grab the package id, if specified. default to
					// property name.
					parts = pluginParts(removeEndSlash(data['id'] || data['name'] || name));
					id = parts.resourceId;
					pluginId = parts.pluginId;
					if (pluginId) {
						// plugin-specific path
						pluginCfg = pluginCfgs[pluginId];
						if (!pluginCfg) {
							pluginCfg = pluginCfgs[pluginId] = beget(cfg);
							pluginCfg.pathMap = beget(cfg.pathMap);
							pluginCfg.pathList = [];
						}
						// remove plugin-specific path from coll
						delete coll[name];
					}
					if (isPkg) {
						info = normalizePkgDescriptor(data);
					}
					else {
						info = { path: removeEndSlash(data) };
					}
					info.specificity = id.split('/').length;
//					info.specificity = (id.match(findSlashRx) || []).length;
					if (id) {
						pluginCfg.pathMap[id] = info;
						pluginCfg.pathList.push(id);
					}
					else {
						// naked plugin name signifies baseUrl for plugin
						// resources. baseUrl could be relative to global
						// baseUrl.
						pluginCfg.baseUrl = core.resolveUrl(data, cfg);
					}
				}
			}

			// adds the path matching regexp onto the cfg or plugin cfgs.
			function convertPathMatcher (cfg) {
				var pathMap = cfg.pathMap;
				cfg.pathRx = new RegExp('^(' +
					cfg.pathList.sort(function (a, b) { return pathMap[a].specificity < pathMap[b].specificity; } )
						.join('|')
						.replace(/\//g, '\\/') +
					')(?=\\/|$)'
				);
				delete cfg.pathList;
			}

			// fix all paths and packages
			fixAndPushPaths(cfg['paths'], false);
			fixAndPushPaths(cfg['packages'], true);

			// create search regex for each path map
			for (var p in pluginCfgs) {
				var pathList = pluginCfgs[p].pathList;
				if (pathList) {
					pluginCfgs[p].pathList = pathList.concat(cfg.pathList);
					convertPathMatcher(pluginCfgs[p]);
				}
			}
			convertPathMatcher(cfg);

			return cfg;

		},

		checkPreloads: function (cfg) {
			var preloads = cfg['preloads'];
			if (preloads && preloads.length > 0){
				// chain from previous preload, if any (revisit when
				// doing package-specific configs).
				when(preload, function () {
					preload = core.createResourceDef(undef, cfg, true);
					core.getDeps(preload, preloads);
				});
			}

		},

		resolvePathInfo: function (id, cfg, forPlugin) {
			// searches through the configured path mappings and packages
			var pathMap, pathInfo, path, pkgCfg, found;

			pathMap = cfg.pathMap;

			if (forPlugin && cfg.pluginPath && id.indexOf('/') < 0 && !(id in pathMap)) {
				// prepend plugin folder path, if it's missing and path isn't in pathMap
				// Note: this munges the concepts of ids and paths for plugins,
				// but is generally safe since it's only for non-namespaced
				// plugins (plugins without path or package info).
				id = joinPath(cfg.pluginPath, id);
			}

			if (!absUrlRx.test(id)) {
				path = id.replace(cfg.pathRx, function (match) {

					pathInfo = pathMap[match] || {};
					found = true;
					pkgCfg = pathInfo.config;

					// if pathInfo.main and match == id, this is a main module
					if (pathInfo.main && match == id) {
						return pathInfo.main;
					}
					// if pathInfo.path return pathInfo.path
					else {
						return pathInfo.path || '';
					}

				});
			}
			else {
				path = id;
			}

			return {
				path: path,
				config: pkgCfg || userCfg,
				url: core.resolveUrl(path, cfg)
			};
		},

		resolveUrl: function (path, cfg) {
			var baseUrl = cfg.baseUrl;
			return baseUrl && !absUrlRx.test(path) ? joinPath(baseUrl, path) : path;
		},

		checkToAddJsExt: function (url) {
			// don't add extension if a ? is found in the url (query params)
			// i'd like to move this feature to a moduleLoader
			return url + (dontAddExtRx.test(url) ? '' : '.js');
		},

		loadScript: function (def, success, failure) {
			// script processing rules learned from RequireJS

			// insert script
			var el = doc.createElement('script');

			// initial script processing
			function process (ev) {
				ev = ev || global.event;
				// detect when it's done loading
				if (ev.type == 'load' || readyStates[el.readyState]) {
					delete activeScripts[def.id];
					// release event listeners
					el.onload = el.onreadystatechange = el.onerror = ''; // ie cries if we use undefined
					success();
				}
			}

			function fail (e) {
				// some browsers send an event, others send a string,
				// but none of them send anything useful, so just say we failed:
				failure(new Error('Syntax or http error: ' + def.url));
			}

			// set type first since setting other properties could
			// prevent us from setting this later
			// actually, we don't even need to set this at all
			//el.type = 'text/javascript';
			// using dom0 event handlers instead of wordy w3c/ms
			el.onload = el.onreadystatechange = process;
			el.onerror = fail;
			// TODO: support other charsets?
			el.charset = 'utf-8';
			el.async = true;
			el.src = def.url;

			// loading will start when the script is inserted into the dom.
			// IE will load the script sync if it's in the cache, so
			// indicate the current resource definition if this happens.
			activeScripts[def.id] = el;
			// use insertBefore to keep IE from throwing Operation Aborted (thx Bryan Forbes!)
			head.insertBefore(el, head.firstChild);

		},

		extractCjsDeps: function (defFunc) {
			// Note: ignores require() inside strings and comments
			var source, ids = [], currQuote;
			// prefer toSource (FF) since it strips comments
			source = typeof defFunc == 'string' ?
					 defFunc :
					 defFunc.toSource ? defFunc.toSource() : defFunc.toString();
			// remove comments, then look for require() or quotes
			source.replace(removeCommentsRx, '').replace(findRValueRequiresRx, function (m, id, qq) {
				// if we encounter a quote
				if (qq) {
					currQuote = currQuote == qq ? undef : currQuote;
				}
				// if we're not inside a quoted string
				else if (!currQuote) {
					ids.push(id);
				}
				return m; // uses least RAM/CPU
			});
			return ids;
		},

		fixArgs: function (args) {
			// resolve args
			// valid combinations for define:
			// (string, array, object|function) sax|saf
			// (array, object|function) ax|af
			// (string, object|function) sx|sf
			// (object|function) x|f

			var id, deps, defFunc, isDefFunc, len, cjs;

			len = args.length;

			defFunc = args[len - 1];
			isDefFunc = isType(defFunc, 'Function');

			if (len == 2) {
				if (isType(args[0], 'Array')) {
					deps = args[0];
				}
				else {
					id = args[0];
				}
			}
			else if (len == 3) {
				id = args[0];
				deps = args[1];
			}

			// Hybrid format: assume that a definition function with zero
			// dependencies and non-zero arity is a wrapped CommonJS module
			if (!deps && isDefFunc && defFunc.length > 0) {
				cjs = true;
				deps = ['require', 'exports', 'module'].concat(core.extractCjsDeps(defFunc));
			}

			return {
				id: id,
				deps: deps || [],
				res: isDefFunc ? defFunc : function () { return defFunc; },
				cjs: cjs
			};
		},

		executeDefFunc: function (def) {
			var resource, moduleThis;
			// the force of AMD is strong so anything returned
			// overrides exports.
			// node.js assumes `this` === `exports` so we do that
			// for all cjs-wrapped modules, just in case.
			// also, use module.exports if that was set
			// (node.js convention).
			// note: if .module exists, .exports exists.
			moduleThis = def.cjs ? def.exports : undef;
			resource = def.res.apply(moduleThis, def.deps);
			if (resource === undef && def.exports) {
				// note: exports will equal module.exports unless
				// module.exports was reassigned inside module.
				resource = def.module ? def.module.exports : def.exports;
			}
			return resource;
		},

		resolveResDef: function (def, args) {

			def.cjs = args.cjs;

			// get the dependencies and then resolve/reject
			core.getDeps(def, args.deps,
				function (deps) {
					var resource;
					def.deps = deps;
					def.res = args.res;
					try {
						resource = core.executeDefFunc(def);
					}
					catch (ex) {
						def.reject(ex);
					}
					def.resolve(resource);
				}
			);

		},

		fetchResDef: function (def) {

			// ensure url is computed
			core.getDefUrl(def);

			core.loadScript(def,

				function () {
					var args = argsNet;
					argsNet = undef; // reset it before we get deps

					// if our resource was not explicitly defined with an id (anonymous)
					// Note: if it did have an id, it will be resolved in the define()
					if (def.useNet !== false) {

						// if !args, nothing was added to the argsNet
						if (!args || args.ex) {
							def.reject(new Error(((args && args.ex) || 'define() missing or duplicated: url').replace('url', def.url)));
						}
						else {
							core.resolveResDef(def, args);
						}
					}

				},

				def.reject

			);

			return def;

		},

		fetchDep: function (depName, parentDef) {
			var toAbsId, isPreload, parts, mainId, loaderId, pluginId,
				resId, pathInfo, def, tempDef, resCfg;

			toAbsId = parentDef.toAbsId;
			isPreload = parentDef.isPreload;

			// check for plugin loaderId
			parts = pluginParts(depName);
			// resId is not normalized since the plugin may need to do it
			resId = parts.resourceId;

			// get id of first resource to load (which could be a plugin)
			mainId = toAbsId(parts.pluginId || resId);
			pathInfo = core.resolvePathInfo(mainId, userCfg, !!parts.pluginId);

			// get custom module loader from package config if not a plugin
			// TODO: figure out how to make module loaders work with plugins
			if (parts.pluginId) {
				loaderId = mainId;
			}
			else {
				loaderId = pathInfo.config['moduleLoader'];
				if (loaderId) {
					// since we're not using toAbsId, transformers must be absolute
					resId = mainId;
					mainId = loaderId;
					pathInfo = core.resolvePathInfo(loaderId, userCfg);
				}
			}

			// find resource definition
			def = cache[mainId];
			if (!def) {
				def = cache[mainId] = core.createResourceDef(mainId, pathInfo.config, isPreload, pathInfo.path);
				def.url = core.checkToAddJsExt(pathInfo.url);
				core.fetchResDef(def);
			}

			// plugin or transformer
			if (mainId == loaderId) {

				// we need to use depName until plugin tells us normalized id.
				// if the plugin changes the id, we need to consolidate
				// def promises below.  Note: exports objects will be different
				// between pre-normalized and post-normalized defs! does this matter?
				// don't put this resource def in the cache because if the
				// resId doesn't change, the check if this is a new
				// normalizedDef (below) will think it's already being loaded.
				tempDef = /*cache[depName] =*/ core.createResourceDef(depName);

				// note: this means moduleLoaders can store config info in the
				// plugins config, too.
				resCfg = userCfg.plugins[loaderId] || userCfg;

				// wait for plugin resource def
				when(def, function(plugin) {
					var normalizedDef, fullId;

					// check if plugin supports the normalize method
					if ('normalize' in plugin) {
						// dojo/has may return falsey values (0, actually)
						resId = plugin['normalize'](resId, toAbsId, resCfg) || '';
					}
					else {
						resId = toAbsId(resId);
					}

					// use the full id (loaderId + id) to id plugin resources
					// so multiple plugins may each process the same resource
					// resId could be blank if the plugin doesn't require any (e.g. "domReady!")
					fullId = loaderId + '!' + resId;
					normalizedDef = cache[fullId];

					// if this is our first time fetching this (normalized) def
					if (!normalizedDef) {

						// because we're using resId, plugins, such as wire!,
						// can use paths relative to the resource
						normalizedDef = core.createResourceDef(fullId, resCfg, isPreload, resId);

						// don't cache non-determinate "dynamic" resources (or non-existent resources)
						if (!plugin['dynamic']) {
							cache[fullId] = normalizedDef;
						}

						// curl's plugins prefer to receive a deferred,
						// but to be compatible with AMD spec, we have to
						// piggy-back on the callback function parameter:
						var loaded = function (res) {
							normalizedDef.resolve(res);
							if (plugin['dynamic']) delete cache[fullId];
						};
						loaded['resolve'] = loaded;
						loaded['reject'] = normalizedDef.reject;

						// load the resource!
						plugin.load(resId, normalizedDef.require, loaded, resCfg);

					}

					// chain defs (resolve when plugin.load executes)
					if (tempDef != normalizedDef) {
						when(normalizedDef, tempDef.resolve, tempDef.reject);
					}

				}, tempDef.reject);

			}

			// return tempDef if this is a plugin-based resource
			return tempDef || def;
		},

		getDeps: function (parentDef, names, overrideCallback) {
			var deps, count, len, i, name, completed, callback;

			deps = [];
			count = len = names.length;
			completed = false;
			callback = overrideCallback || parentDef.resolve;

			function checkDone () {
				if (--count == 0) {
					// Note: IE may have obtained the dependencies sync, thus the completed flag
					completed = true;
					callback(deps);
				}
			}

			function getDep (index, depName) {
				var childDef, doOnce;

				childDef = core.fetchDep(depName, parentDef);

				doOnce = function (dep) {
					deps[index] = dep; // got it!
					checkDone();
					// only run once for this dep (in case of early exports)
					doOnce = noop;
				};

				function doSuccess (dep) {
					doOnce(dep);
				}

				function doFailure (ex) {
					completed = true;
					parentDef.reject(ex);
				}

				function doProgress (msg) {
					// only early-export to modules that also export since
					// pure AMD modules don't expect to get an early export
					// Note: this logic makes dojo 1.7 work, too.
					if (msg == msgUsingExports && parentDef.exports) {
						doOnce(childDef.exports);
					}
				}

				// hook into promise callbacks.
				when(childDef, doSuccess, doFailure, doProgress);

			}

			// wait for preload before fetching any other modules
			when(parentDef.isPreload || preload, function () {

				for (i = 0; i < len && !completed; i++) {
					name = names[i];
					if (name in cjsGetters) {
						// is this "require", "exports", or "module"?
						deps[i] = cjsGetters[name](parentDef);
						checkDone();
					}
					// check for blanks. fixes #32.
					// this helps support yepnope.js, has.js, and the has! plugin
					else if (names[i]) {
						getDep(i, names[i]);
					}
					else {
						checkDone();
					}
				}

				if (parentDef.exports) {
					// announce
					parentDef.progress(msgUsingExports);
				}

				if (count == 0 && !completed) {
					// there were none to fetch
					callback(deps);
				}

			});

		},

		getCurrentDefName: function () {
			// IE marks the currently executing thread as "interactive"
			// Note: Opera lies about which scripts are "interactive", so we
			// just have to test for it. Opera provides a true browser test, not
			// a UA sniff, thankfully.
			// learned this trick from James Burke's RequireJS
			var def;
			if (!isType(global.opera, 'Opera')) {
				for (var d in activeScripts) {
					if (readyStates[activeScripts[d].readyState] == interactive) {
						def = d;
						break;
					}
				}
			}
			return def;
		}

	};

	// hook-up cjs free variable getters
	cjsGetters = {'require': core.getCjsRequire, 'exports': core.getCjsExports, 'module': core.getCjsModule};

	function _curl (/* various */) {

		var args = [].slice.call(arguments), ids;

		// extract config, if it's specified
		if (isType(args[0], 'Object')) {
			userCfg = core.extractCfg(args.shift());
			core.checkPreloads(userCfg);
		}

		// thanks to Joop Ringelberg for helping troubleshoot the API
		function CurlApi (ids, callback, waitFor) {
			var then, def;
			def = core.createResourceDef(undef, userCfg);
			this['then'] = then = function (resolved, rejected) {
				when(def,
					// return the dependencies as arguments, not an array
					function (deps) { if (resolved) resolved.apply(undef, deps); },
					// just throw if the dev didn't specify an error handler
					function (ex) { if (rejected) rejected(ex); else throw ex; }
				);
				return this;
			};
			this['next'] = function (ids, cb) {
				// chain api
				return new CurlApi(ids, cb, def);
			};
			if (callback) then(callback);
			when(waitFor, function () {
				core.getDeps(def, [].concat(ids));
			});
		}

		ids = [].concat(args[0]); // force to array TODO: create unit test
		return new CurlApi(ids, args[1]);

	}

	function _define (args) {

		var id = args.id;

		if (id == undef) {
			if (argsNet !== undef) {
				argsNet = {ex: 'Multiple anonymous defines in url'};
			}
			else if (!(id = core.getCurrentDefName())/* intentional assignment */) {
				// anonymous define(), defer processing until after script loads
				argsNet = args;
			}
		}
		if (id != undef) {
			// named define(), it is in the cache if we are loading a dependency
			// (could also be a secondary define() appearing in a built file, etc.)
			var def = cache[id];
			if (!def) {
				// id is an absolute id in this case, so we can get the config.
				// there's no way to allow a named define to fetch dependencies
				// in the preload phase since we can't cascade the parent def.
				var cfg = core.resolvePathInfo(id, userCfg).config;
				def = cache[id] = core.createResourceDef(id, cfg);
			}
			// check if this resource has already been resolved (can happen if
			// a module was defined inside a built file and outside of it and
			// dev didn't coordinate it explicitly)
			if (isPromise(def)) {
				def.useNet = false;
				core.resolveResDef(def, args);
			}
		}

	}

	/***** grab any global configuration info *****/

	// if userCfg is a function, assume curl() exists already
	if (isType(userCfg, 'Function')) return;

	userCfg = core.extractCfg(userCfg || {});
	core.checkPreloads(userCfg);

	/***** define public API *****/

	var apiName, apiContext, define;

	// allow curl to be renamed and added to a specified context
	apiName = userCfg['apiName'] || 'curl';
	apiContext = userCfg['apiContext'] || global;
	apiContext[apiName] = _curl;

	// allow curl to be a dependency
	cache['curl'] = _curl;

	// wrap inner _define so it can be replaced without losing define.amd
	define = global['define'] = function () {
		var args = core.fixArgs(arguments);
		_define(args);
	};
	_curl['version'] = version;

	// indicate our capabilities:
	define['amd'] = { 'plugins': true, 'jQuery': true, 'curl': version };

	// expose curl core for special plugins and modules
	// Note: core overrides will only work in either of two scenarios:
	// 1. the files are running un-compressed (Google Closure or Uglify)
	// 2. the overriding module was compressed with curl.js
	// Compiling curl and the overriding module separately won't work.
	cache['curl/_privileged'] = {
		'core': core,
		'cache': cache,
		'cfg': userCfg,
		'_define': _define,
		'_curl': _curl,
		'ResourceDef': ResourceDef
	};

}(this));