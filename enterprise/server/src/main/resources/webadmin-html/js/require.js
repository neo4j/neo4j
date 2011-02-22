/*
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

/** vim: et:ts=4:sw=4:sts=4
 * @license RequireJS 0.23.0 Copyright (c) 2010-2011, The Dojo Foundation All Rights Reserved.
 * Available via the MIT or new BSD license.
 * see: http://github.com/jrburke/requirejs for details
 */
/*jslint white: false, strict: false, plusplus: false */
/*global window: false, navigator: false, document: false, importScripts: false,
  jQuery: false, clearInterval: false, setInterval: false, self: false,
  setTimeout: false */

var require, define;
(function () {
    //Change this version number for each release.
    var version = "0.23.0",
        commentRegExp = /(\/\*([\s\S]*?)\*\/|\/\/(.*)$)/mg,
        cjsRequireRegExp = /require\(["']([^'"\s]+)["']\)/g,
        currDirRegExp = /^\.\//,
        jsSuffixRegExp = /\.js$/,
        ostring = Object.prototype.toString,
        ap = Array.prototype,
        aps = ap.slice,
        apsp = ap.splice,
        isBrowser = !!(typeof window !== "undefined" && navigator && document),
        isWebWorker = !isBrowser && typeof importScripts !== "undefined",
        //PS3 indicates loaded and complete, but need to wait for complete
        //specifically. Sequence is "loading", "loaded", execution,
        // then "complete". The UA check is unfortunate, but not sure how
        //to feature test w/o causing perf issues.
        readyRegExp = isBrowser && navigator.platform === 'PLAYSTATION 3' ?
                      /^complete$/ : /^(complete|loaded)$/,
        defContextName = "_",
        reqWaitIdPrefix = "_r@@",
        empty = {},
        contexts = {},
        globalDefQueue = [],
        interactiveScript = null,
        isDone = false,
        useInteractive = false,
        //Default plugins that have remapped names, if no mapping
        //already exists.
        requirePlugins = {
            "text": "require/text",
            "i18n": "require/i18n",
            "order": "require/order"
        },
        req, cfg = {}, currentlyAddingScript, s, head, baseElement, scripts, script,
        src, subPath, mainScript, dataMain, i, scrollIntervalId, setReadyState, ctx;

    function isFunction(it) {
        return ostring.call(it) === "[object Function]";
    }

    function isArray(it) {
        return ostring.call(it) === "[object Array]";
    }

    /**
     * Simple function to mix in properties from source into target,
     * but only if target does not already have a property of the same name.
     * This is not robust in IE for transferring methods that match
     * Object.prototype names, but the uses of mixin here seem unlikely to
     * trigger a problem related to that.
     */
    function mixin(target, source, force) {
        for (var prop in source) {
            if (!(prop in empty) && (!(prop in target) || force)) {
                target[prop] = source[prop];
            }
        }
        return req;
    }

    /**
     * Used to set up package paths from a packagePaths or packages config object.
     * @param {Object} pkgs the object to store the new package config
     * @param {Array} currentPackages an array of packages to configure
     * @param {String} [dir] a prefix dir to use.
     */
    function configurePackageDir(pkgs, currentPackages, dir) {
        var i, location, pkgObj;

        for (i = 0; (pkgObj = currentPackages[i]); i++) {
            pkgObj = typeof pkgObj === "string" ? { name: pkgObj } : pkgObj;
            location = pkgObj.location;

            //Add dir to the path, but avoid paths that start with a slash
            //or have a colon (indicates a protocol)
            if (dir && (!location || (location.indexOf("/") !== 0 && location.indexOf(":") === -1))) {
                location = dir + "/" + (location || pkgObj.name);
            }

            //Create a brand new object on pkgs, since currentPackages can
            //be passed in again, and config.pkgs is the internal transformed
            //state for all package configs.
            pkgs[pkgObj.name] = {
                name: pkgObj.name,
                location: location || pkgObj.name,
                lib: pkgObj.lib || "lib",
                //Remove leading dot in main, so main paths are normalized,
                //and remove any trailing .js, since different package
                //envs have different conventions: some use a module name,
                //some use a file name.
                main: (pkgObj.main || "lib/main")
                      .replace(currDirRegExp, '')
                      .replace(jsSuffixRegExp, '')
            };
        }
    }

    //Check for an existing version of require. If so, then exit out. Only allow
    //one version of require to be active in a page. However, allow for a require
    //config object, just exit quickly if require is an actual function.
    if (typeof require !== "undefined") {
        if (isFunction(require)) {
            return;
        } else {
            //assume it is a config object.
            cfg = require;
        }
    }

    /**
     * Creates a new context for use in require and define calls.
     * Handle most of the heavy lifting. Do not want to use an object
     * with prototype here to avoid using "this" in require, in case it
     * needs to be used in more super secure envs that do not want this.
     * Also there should not be that many contexts in the page. Usually just
     * one for the default context, but could be extra for multiversion cases
     * or if a package needs a special context for a dependency that conflicts
     * with the standard context.
     */
    function newContext(contextName) {
        var context, resume,
            config = {
                waitSeconds: 7,
                baseUrl: s.baseUrl || "./",
                paths: {},
                pkgs: {}
            },
            defQueue = [],
            specified = {
                "require": true,
                "exports": true,
                "module": true
            },
            urlMap = {},
            defined = {},
            loaded = {},
            waiting = {},
            waitAry = [],
            waitIdCounter = 0,
            managerCallbacks = {},
            plugins = {},
            pluginsQueue = {},
            resumeDepth = 0,
            normalizedWaiting = {};

        /**
         * Trims the . and .. from an array of path segments.
         * It will keep a leading path segment if a .. will become
         * the first path segment, to help with module name lookups,
         * which act like paths, but can be remapped. But the end result,
         * all paths that use this function should look normalized.
         * NOTE: this method MODIFIES the input array.
         * @param {Array} ary the array of path segments.
         */
        function trimDots(ary) {
            var i, part;
            for (i = 0; (part = ary[i]); i++) {
                if (part === ".") {
                    ary.splice(i, 1);
                    i -= 1;
                } else if (part === "..") {
                    if (i === 1 && (ary[2] === '..' || ary[0] === '..')) {
                        //End of the line. Keep at least one non-dot
                        //path segment at the front so it can be mapped
                        //correctly to disk. Otherwise, there is likely
                        //no path mapping for a path starting with '..'.
                        //This can still fail, but catches the most reasonable
                        //uses of ..
                        break;
                    } else if (i > 0) {
                        ary.splice(i - 1, 2);
                        i -= 2;
                    }
                }
            }
        }

        /**
         * Given a relative module name, like ./something, normalize it to
         * a real name that can be mapped to a path.
         * @param {String} name the relative name
         * @param {String} baseName a real name that the name arg is relative
         * to.
         * @returns {String} normalized name
         */
        function normalize(name, baseName) {
            var pkgName, pkgConfig;

            //Adjust any relative paths.
            if (name.charAt(0) === ".") {
                //If have a base name, try to normalize against it,
                //otherwise, assume it is a top-level require that will
                //be relative to baseUrl in the end.
                if (baseName) {
                    if (config.pkgs[baseName]) {
                        //If the baseName is a package name, then just treat it as one
                        //name to concat the name with.
                        baseName = [baseName];
                    } else {
                        //Convert baseName to array, and lop off the last part,
                        //so that . matches that "directory" and not name of the baseName's
                        //module. For instance, baseName of "one/two/three", maps to
                        //"one/two/three.js", but we want the directory, "one/two" for
                        //this normalization.
                        baseName = baseName.split("/");
                        baseName = baseName.slice(0, baseName.length - 1);
                    }

                    name = baseName.concat(name.split("/"));
                    trimDots(name);

                    //Some use of packages may use a . path to reference the
                    //"main" module name, so normalize for that.
                    pkgConfig = config.pkgs[(pkgName = name[0])];
                    name = name.join("/");
                    if (pkgConfig && name === pkgName + '/' + pkgConfig.main) {
                        name = pkgName;
                    }
                }
            }
            return name;
        }

        /**
         * Creates a module mapping that includes plugin prefix, module
         * name, and path. If parentModuleMap is provided it will
         * also normalize the name via require.normalize()
         *
         * @param {String} name the module name
         * @param {String} [parentModuleMap] parent module map
         * for the module name, used to resolve relative names.
         *
         * @returns {Object}
         */
        function makeModuleMap(name, parentModuleMap) {
            var index = name ? name.indexOf("!") : -1,
                prefix = null,
                parentName = parentModuleMap ? parentModuleMap.name : null,
                originalName = name,
                normalizedName, url, pluginModule;

            if (index !== -1) {
                prefix = name.substring(0, index);
                name = name.substring(index + 1, name.length);
            }

            if (prefix) {
                prefix = normalize(prefix, parentName);
                //Allow simpler mappings for some plugins
                prefix = requirePlugins[prefix] || prefix;
            }

            //Account for relative paths if there is a base name.
            if (name) {
                if (prefix) {
                    pluginModule = defined[prefix];
                    if (pluginModule) {
                        //Plugin is loaded, use its normalize method, otherwise,
                        //normalize name as usual.
                        if (pluginModule.normalize) {
                            normalizedName = pluginModule.normalize(name, function (name) {
                                return normalize(name, parentName);
                            });
                        } else {
                            normalizedName = normalize(name, parentName);
                        }
                    } else {
                        //Plugin is not loaded yet, so do not normalize
                        //the name, wait for plugin to load to see if
                        //it has a normalize method. To avoid possible
                        //ambiguity with relative names loaded from another
                        //plugin, use the parent's name as part of this name.
                        normalizedName = '__$p' + parentName + '@' + name;
                    }
                } else {
                    normalizedName = normalize(name, parentName);
                }

                url = urlMap[normalizedName];
                if (!url) {
                    //Calculate url for the module, if it has a name.
                    if (req.toModuleUrl) {
                        //Special logic required for a particular engine,
                        //like Node.
                        url = req.toModuleUrl(context, name, parentModuleMap);
                    } else {
                        url = context.nameToUrl(name, null, parentModuleMap);
                    }

                    //Store the URL mapping for later.
                    urlMap[normalizedName] = url;
                }
            }

            return {
                prefix: prefix,
                name: normalizedName,
                parentMap: parentModuleMap,
                url: url,
                originalName: originalName,
                fullName: prefix ? prefix + "!" + normalizedName : normalizedName
            };
        }

        /**
         * Determine if priority loading is done. If so clear the priorityWait
         */
        function isPriorityDone() {
            var priorityDone = true,
                priorityWait = config.priorityWait,
                priorityName, i;
            if (priorityWait) {
                for (i = 0; (priorityName = priorityWait[i]); i++) {
                    if (!loaded[priorityName]) {
                        priorityDone = false;
                        break;
                    }
                }
                if (priorityDone) {
                    delete config.priorityWait;
                }
            }
            return priorityDone;
        }

        /**
         * Helper function that creates a setExports function for a "module"
         * CommonJS dependency. Do this here to avoid creating a closure that
         * is part of a loop.
         */
        function makeSetExports(moduleObj) {
            return function (exports) {
                moduleObj.exports = exports;
            };
        }

        function makeContextModuleFunc(func, relModuleMap, enableBuildCallback) {
            return function () {
                //A version of a require function that passes a moduleName
                //value for items that may need to
                //look up paths relative to the moduleName
                var args = [].concat(aps.call(arguments, 0)), lastArg;
                if (enableBuildCallback &&
                    isFunction((lastArg = args[args.length - 1]))) {
                    lastArg.__requireJsBuild = true;
                }
                args.push(relModuleMap);
                return func.apply(null, args);
            };
        }

        /**
         * Helper function that creates a require function object to give to
         * modules that ask for it as a dependency. It needs to be specific
         * per module because of the implication of path mappings that may
         * need to be relative to the module name.
         */
        function makeRequire(relModuleMap, enableBuildCallback) {
            var modRequire = makeContextModuleFunc(context.require, relModuleMap, enableBuildCallback);

            mixin(modRequire, {
                nameToUrl: makeContextModuleFunc(context.nameToUrl, relModuleMap),
                toUrl: makeContextModuleFunc(context.toUrl, relModuleMap),
                isDefined: makeContextModuleFunc(context.isDefined, relModuleMap),
                ready: req.ready,
                isBrowser: req.isBrowser
            });
            //Something used by node.
            if (req.paths) {
                modRequire.paths = req.paths;
            }
            return modRequire;
        }

        /**
         * Used to update the normalized name for plugin-based dependencies
         * after a plugin loads, since it can have its own normalization structure.
         * @param {String} pluginName the normalized plugin module name.
         */
        function updateNormalizedNames(pluginName) {

            var oldFullName, oldModuleMap, moduleMap, fullName, callbacks,
                i, j, k, depArray, existingCallbacks,
                maps = normalizedWaiting[pluginName];

            if (maps) {
                for (i = 0; (oldModuleMap = maps[i]); i++) {
                    oldFullName = oldModuleMap.fullName;
                    moduleMap = makeModuleMap(oldModuleMap.originalName, oldModuleMap.parentMap);
                    fullName = moduleMap.fullName;
                    callbacks = managerCallbacks[oldFullName];
                    existingCallbacks = managerCallbacks[fullName];

                    if (fullName !== oldFullName) {
                        //Update the specified object, but only if it is already
                        //in there. In sync environments, it may not be yet.
                        if (oldFullName in specified) {
                            delete specified[oldFullName];
                            specified[fullName] = true;
                        }

                        //Update managerCallbacks to use the correct normalized name.
                        //If there are already callbacks for the normalized name,
                        //just add to them.
                        if (existingCallbacks) {
                            managerCallbacks[fullName] = existingCallbacks.concat(callbacks);
                        } else {
                            managerCallbacks[fullName] = callbacks;
                        }
                        delete managerCallbacks[oldFullName];

                        //In each manager callback, update the normalized name in the depArray.
                        for (j = 0; j < callbacks.length; j++) {
                            depArray = callbacks[j].depArray;
                            for (k = 0; k < depArray.length; k++) {
                                if (depArray[k] === oldFullName) {
                                    depArray[k] = fullName;
                                }
                            }
                        }
                    }
                }
            }

            delete normalizedWaiting[pluginName];
        }

        /*
         * Queues a dependency for checking after the loader is out of a
         * "paused" state, for example while a script file is being loaded
         * in the browser, where it may have many modules defined in it.
         *
         * depName will be fully qualified, no relative . or .. path.
         */
        function queueDependency(dep) {
            //Make sure to load any plugin and associate the dependency
            //with that plugin.
            var prefix = dep.prefix,
                fullName = dep.fullName;

            //Do not bother if the depName is already in transit
            if (specified[fullName] || fullName in defined) {
                return;
            }

            if (prefix && !plugins[prefix]) {
                //Queue up loading of the dependency, track it
                //via context.plugins. Mark it as a plugin so
                //that the build system will know to treat it
                //special.
                plugins[prefix] = undefined;

                //Remember this dep that needs to have normaliztion done
                //after the plugin loads.
                (normalizedWaiting[prefix] || (normalizedWaiting[prefix] = []))
                    .push(dep);

                //Register an action to do once the plugin loads, to update
                //all managerCallbacks to use a properly normalized module
                //name.
                (managerCallbacks[prefix] ||
                (managerCallbacks[prefix] = [])).push({
                    onDep: function (name, value) {
                        if (name === prefix) {
                            updateNormalizedNames(prefix);
                        }
                    }
                });

                queueDependency(makeModuleMap(prefix));
            }

            context.paused.push(dep);
        }

        function execManager(manager) {
            var i, ret, waitingCallbacks,
                cb = manager.callback,
                fullName = manager.fullName,
                args = [],
                ary = manager.depArray;

            //Call the callback to define the module, if necessary.
            if (cb && isFunction(cb)) {
                //Pull out the defined dependencies and pass the ordered
                //values to the callback.
                if (ary) {
                    for (i = 0; i < ary.length; i++) {
                        args.push(manager.deps[ary[i]]);
                    }
                }

                ret = req.execCb(fullName, manager.callback, args);

                if (fullName) {
                    //If using exports and the function did not return a value,
                    //and the "module" object for this definition function did not
                    //define an exported value, then use the exports object.
                    if (manager.usingExports && ret === undefined && (!manager.cjsModule || !("exports" in manager.cjsModule))) {
                        ret = defined[fullName];
                    } else {
                        if (manager.cjsModule && "exports" in manager.cjsModule) {
                            ret = defined[fullName] = manager.cjsModule.exports;
                        } else {
                            if (fullName in defined && !manager.usingExports) {
                                return req.onError(new Error(fullName + " has already been defined"));
                            }
                            defined[fullName] = ret;
                        }
                    }
                }
            } else if (fullName) {
                //May just be an object definition for the module. Only
                //worry about defining if have a module name.
                ret = defined[fullName] = cb;
            }

            if (fullName) {
                //If anything was waiting for this module to be defined,
                //notify them now.
                waitingCallbacks = managerCallbacks[fullName];
                if (waitingCallbacks) {
                    for (i = 0; i < waitingCallbacks.length; i++) {
                        waitingCallbacks[i].onDep(fullName, ret);
                    }
                    delete managerCallbacks[fullName];
                }
            }

            //Clean up waiting.
            if (waiting[manager.waitId]) {
                delete waiting[manager.waitId];
                manager.isDone = true;
                context.waitCount -= 1;
                if (context.waitCount === 0) {
                    //Clear the wait array used for cycles.
                    waitAry = [];
                }
            }

            return undefined;
        }

        function main(inName, depArray, callback, relModuleMap) {
            var moduleMap = makeModuleMap(inName, relModuleMap),
                name = moduleMap.name,
                fullName = moduleMap.fullName,
                uniques = {},
                manager = {
                    //Use a wait ID because some entries are anon
                    //async require calls.
                    waitId: name || reqWaitIdPrefix + (waitIdCounter++),
                    depCount: 0,
                    depMax: 0,
                    prefix: moduleMap.prefix,
                    name: name,
                    fullName: fullName,
                    deps: {},
                    depArray: depArray,
                    callback: callback,
                    onDep: function (depName, value) {
                        if (!(depName in manager.deps)) {
                            manager.deps[depName] = value;
                            manager.depCount += 1;
                            if (manager.depCount === manager.depMax) {
                                //All done, execute!
                                execManager(manager);
                            }
                        }
                    }
                },
                i, depArg, depName, cjsMod;

            if (fullName) {
                //If module already defined for context, or already loaded,
                //then leave.
                if (fullName in defined || loaded[fullName] === true) {
                    return;
                }

                //Set specified/loaded here for modules that are also loaded
                //as part of a layer, where onScriptLoad is not fired
                //for those cases. Do this after the inline define and
                //dependency tracing is done.
                //Also check if auto-registry of jQuery needs to be skipped.
                specified[fullName] = true;
                loaded[fullName] = true;
                context.jQueryDef = (fullName === "jquery");
            }

            //Add the dependencies to the deps field, and register for callbacks
            //on the dependencies.
            for (i = 0; i < depArray.length; i++) {
                depArg = depArray[i];
                //There could be cases like in IE, where a trailing comma will
                //introduce a null dependency, so only treat a real dependency
                //value as a dependency.
                if (depArg) {
                    //Split the dependency name into plugin and name parts
                    depArg = makeModuleMap(depArg, (name ? moduleMap : relModuleMap));
                    depName = depArg.fullName;

                    //Fix the name in depArray to be just the name, since
                    //that is how it will be called back later.
                    depArray[i] = depName;

                    //Fast path CommonJS standard dependencies.
                    if (depName === "require") {
                        manager.deps[depName] = makeRequire(moduleMap);
                    } else if (depName === "exports") {
                        //CommonJS module spec 1.1
                        manager.deps[depName] = defined[fullName] = {};
                        manager.usingExports = true;
                    } else if (depName === "module") {
                        //CommonJS module spec 1.1
                        manager.cjsModule = cjsMod = manager.deps[depName] = {
                            id: name,
                            uri: name ? context.nameToUrl(name, null, relModuleMap) : undefined
                        };
                        cjsMod.setExports = makeSetExports(cjsMod);
                    } else if (depName in defined && !(depName in waiting)) {
                        //Module already defined, no need to wait for it.
                        manager.deps[depName] = defined[depName];
                    } else if (!uniques[depName]) {

                        //A dynamic dependency.
                        manager.depMax += 1;

                        queueDependency(depArg);

                        //Register to get notification when dependency loads.
                        (managerCallbacks[depName] ||
                        (managerCallbacks[depName] = [])).push(manager);

                        uniques[depName] = true;
                    }
                }
            }

            //Do not bother tracking the manager if it is all done.
            if (manager.depCount === manager.depMax) {
                //All done, execute!
                execManager(manager);
            } else {
                waiting[manager.waitId] = manager;
                waitAry.push(manager);
                context.waitCount += 1;
            }
        }

        /**
         * Convenience method to call main for a require.def call that was put on
         * hold in the defQueue.
         */
        function callDefMain(args) {
            main.apply(null, args);
            //Mark the module loaded. Must do it here in addition
            //to doing it in require.def in case a script does
            //not call require.def
            loaded[args[0]] = true;
        }

        /**
         * As of jQuery 1.4.3, it supports a readyWait property that will hold off
         * calling jQuery ready callbacks until all scripts are loaded. Be sure
         * to track it if readyWait is available. Also, since jQuery 1.4.3 does
         * not register as a module, need to do some global inference checking.
         * Even if it does register as a module, not guaranteed to be the precise
         * name of the global. If a jQuery is tracked for this context, then go
         * ahead and register it as a module too, if not already in process.
         */
        function jQueryCheck(jqCandidate) {
            if (!context.jQuery) {
                var $ = jqCandidate || (typeof jQuery !== "undefined" ? jQuery : null);
                if ($ && "readyWait" in $) {
                    context.jQuery = $;

                    //Manually create a "jquery" module entry if not one already
                    //or in process.
                    callDefMain(["jquery", [], function () {
                        return jQuery;
                    }]);

                    //Increment jQuery readyWait if ncecessary.
                    if (context.scriptCount) {
                        $.readyWait += 1;
                        context.jQueryIncremented = true;
                    }
                }
            }
        }

        function forceExec(manager, traced) {
            if (manager.isDone) {
                return undefined;
            }

            var fullName = manager.fullName,
                depArray = manager.depArray,
                depName, i;
            if (fullName) {
                if (traced[fullName]) {
                    return defined[fullName];
                }

                traced[fullName] = true;
            }

            //forceExec all of its dependencies.
            for (i = 0; i < depArray.length; i++) {
                //Some array members may be null, like if a trailing comma
                //IE, so do the explicit [i] access and check if it has a value.
                depName = depArray[i];
                if (depName) {
                    if (!manager.deps[depName] && waiting[depName]) {
                        manager.onDep(depName, forceExec(waiting[depName], traced));
                    }
                }
            }

            return fullName ? defined[fullName] : undefined;
        }

        /**
         * Checks if all modules for a context are loaded, and if so, evaluates the
         * new ones in right dependency order.
         *
         * @private
         */
        function checkLoaded() {
            var waitInterval = config.waitSeconds * 1000,
                //It is possible to disable the wait interval by using waitSeconds of 0.
                expired = waitInterval && (context.startTime + waitInterval) < new Date().getTime(),
                noLoads = "", hasLoadedProp = false, stillLoading = false, prop,
                err, manager;

            //If there are items still in the paused queue processing wait.
            //This is particularly important in the sync case where each paused
            //item is processed right away but there may be more waiting.
            if (context.pausedCount > 0) {
                return undefined;
            }

            //Determine if priority loading is done. If so clear the priority. If
            //not, then do not check
            if (config.priorityWait) {
                if (isPriorityDone()) {
                    //Call resume, since it could have
                    //some waiting dependencies to trace.
                    resume();
                } else {
                    return undefined;
                }
            }

            //See if anything is still in flight.
            for (prop in loaded) {
                if (!(prop in empty)) {
                    hasLoadedProp = true;
                    if (!loaded[prop]) {
                        if (expired) {
                            noLoads += prop + " ";
                        } else {
                            stillLoading = true;
                            break;
                        }
                    }
                }
            }

            //Check for exit conditions.
            if (!hasLoadedProp && !context.waitCount) {
                //If the loaded object had no items, then the rest of
                //the work below does not need to be done.
                return undefined;
            }
            if (expired && noLoads) {
                //If wait time expired, throw error of unloaded modules.
                err = new Error("require.js load timeout for modules: " + noLoads);
                err.requireType = "timeout";
                err.requireModules = noLoads;
                return req.onError(err);
            }
            if (stillLoading || context.scriptCount) {
                //Something is still waiting to load. Wait for it.
                if (isBrowser || isWebWorker) {
                    setTimeout(checkLoaded, 50);
                }
                return undefined;
            }

            //If still have items in the waiting cue, but all modules have
            //been loaded, then it means there are some circular dependencies
            //that need to be broken.
            //However, as a waiting thing is fired, then it can add items to
            //the waiting cue, and those items should not be fired yet, so
            //make sure to redo the checkLoaded call after breaking a single
            //cycle, if nothing else loaded then this logic will pick it up
            //again.
            if (context.waitCount) {
                //Cycle through the waitAry, and call items in sequence.
                for (i = 0; (manager = waitAry[i]); i++) {
                    forceExec(manager, {});
                }

                checkLoaded();
                return undefined;
            }

            //Check for DOM ready, and nothing is waiting across contexts.
            req.checkReadyState();

            return undefined;
        }

        function callPlugin(pluginName, dep) {
            var name = dep.name,
                fullName = dep.fullName;

            //Do not bother if plugin is already defined or being loaded.
            if (fullName in defined || fullName in loaded) {
                return;
            }

            if (!plugins[pluginName]) {
                plugins[pluginName] = defined[pluginName];
            }

            //Only set loaded to false for tracking if it has not already been set.
            if (!loaded[fullName]) {
                loaded[fullName] = false;
            }

            //Use parentName here since the plugin's name is not reliable,
            //could be some weird string with no path that actually wants to
            //reference the parentName's path.
            plugins[pluginName].load(name, makeRequire(dep.parentMap, true), function (ret) {
                //Allow the build process to register plugin-loaded dependencies.
                if (require.onPluginLoad) {
                    require.onPluginLoad(context, pluginName, name, ret);
                }

                execManager({
                    prefix: dep.prefix,
                    name: dep.name,
                    fullName: dep.fullName,
                    callback: function () {
                        return ret;
                    }
                });
                loaded[fullName] = true;
            }, config);
        }

        function loadPaused(dep) {
            //Renormalize dependency if its name was waiting on a plugin
            //to load, which as since loaded.
            if (dep.prefix && dep.name.indexOf('__$p') === 0 && defined[dep.prefix]) {
                dep = makeModuleMap(dep.originalName, dep.parentMap);
            }

            var pluginName = dep.prefix,
                fullName = dep.fullName;

            //Do not bother if the dependency has already been specified.
            if (specified[fullName] || fullName in defined) {
                return;
            } else {
                specified[fullName] = true;
            }

            if (pluginName) {
                //If plugin not loaded, wait for it.
                //set up callback list. if no list, then register
                //managerCallback for that plugin.
                if (defined[pluginName]) {
                    callPlugin(pluginName, dep);
                } else {
                    if (!pluginsQueue[pluginName]) {
                        pluginsQueue[pluginName] = [];
                        (managerCallbacks[pluginName] ||
                        (managerCallbacks[pluginName] = [])).push({
                            onDep: function (name, value) {
                                if (name === pluginName) {
                                    var i, oldModuleMap, ary = pluginsQueue[pluginName];

                                    //Now update all queued plugin actions.
                                    for (i = 0; i < ary.length; i++) {
                                        oldModuleMap = ary[i];
                                        //Update the moduleMap since the
                                        //module name may be normalized
                                        //differently now.
                                        callPlugin(pluginName,
                                                   makeModuleMap(oldModuleMap.originalName, oldModuleMap.parentMap));
                                    }
                                    delete pluginsQueue[pluginName];
                                }
                            }
                        });
                    }
                    pluginsQueue[pluginName].push(dep);
                }
            } else {
                req.load(context, fullName, dep.url);
            }
        }

        /**
         * Resumes tracing of dependencies and then checks if everything is loaded.
         */
        resume = function () {
            var args, i, p;

            resumeDepth += 1;

            if (context.scriptCount <= 0) {
                //Synchronous envs will push the number below zero with the
                //decrement above, be sure to set it back to zero for good measure.
                //require() calls that also do not end up loading scripts could
                //push the number negative too.
                context.scriptCount = 0;
            }

            //Make sure any remaining defQueue items get properly processed.
            while (defQueue.length) {
                args = defQueue.shift();
                if (args[0] === null) {
                    return req.onError(new Error('Mismatched anonymous require.def modules'));
                } else {
                    callDefMain(args);
                }
            }

            //Skip the resume if current context is in priority wait.
            if (config.priorityWait && !isPriorityDone()) {
                return undefined;
            }

            while (context.paused.length) {
                p = context.paused;
                context.pausedCount += p.length;
                //Reset paused list
                context.paused = [];

                for (i = 0; (args = p[i]); i++) {
                    loadPaused(args);
                }
                //Move the start time for timeout forward.
                context.startTime = (new Date()).getTime();
                context.pausedCount -= p.length;
            }

            //Only check if loaded when resume depth is 1. It is likely that
            //it is only greater than 1 in sync environments where a factory
            //function also then calls the callback-style require. In those
            //cases, the checkLoaded should not occur until the resume
            //depth is back at the top level.
            if (resumeDepth === 1) {
                checkLoaded();
            }

            resumeDepth -= 1;

            return undefined;
        };

        //Define the context object. Many of these fields are on here
        //just to make debugging easier.
        context = {
            contextName: contextName,
            config: config,
            defQueue: defQueue,
            waiting: waiting,
            waitCount: 0,
            specified: specified,
            loaded: loaded,
            urlMap: urlMap,
            scriptCount: 0,
            urlFetched: {},
            defined: defined,
            paused: [],
            pausedCount: 0,
            plugins: plugins,
            managerCallbacks: managerCallbacks,
            makeModuleMap: makeModuleMap,
            normalize: normalize,
            /**
             * Set a configuration for the context.
             * @param {Object} cfg config object to integrate.
             */
            configure: function (cfg) {
                var paths, prop, packages, pkgs, packagePaths, requireWait;

                //Make sure the baseUrl ends in a slash.
                if (cfg.baseUrl) {
                    if (cfg.baseUrl.charAt(cfg.baseUrl.length - 1) !== "/") {
                        cfg.baseUrl += "/";
                    }
                }

                //Save off the paths and packages since they require special processing,
                //they are additive.
                paths = config.paths;
                packages = config.packages;
                pkgs = config.pkgs;

                //Mix in the config values, favoring the new values over
                //existing ones in context.config.
                mixin(config, cfg, true);

                //Adjust paths if necessary.
                if (cfg.paths) {
                    for (prop in cfg.paths) {
                        if (!(prop in empty)) {
                            paths[prop] = cfg.paths[prop];
                        }
                    }
                    config.paths = paths;
                }

                packagePaths = cfg.packagePaths;
                if (packagePaths || cfg.packages) {
                    //Convert packagePaths into a packages config.
                    if (packagePaths) {
                        for (prop in packagePaths) {
                            if (!(prop in empty)) {
                                configurePackageDir(pkgs, packagePaths[prop], prop);
                            }
                        }
                    }

                    //Adjust packages if necessary.
                    if (cfg.packages) {
                        configurePackageDir(pkgs, cfg.packages);
                    }

                    //Done with modifications, assing packages back to context config
                    config.pkgs = pkgs;
                }

                //If priority loading is in effect, trigger the loads now
                if (cfg.priority) {
                    //Create a separate config property that can be
                    //easily tested for config priority completion.
                    //Do this instead of wiping out the config.priority
                    //in case it needs to be inspected for debug purposes later.
                    //Hold on to requireWait value, and reset it after done
                    requireWait = context.requireWait;
                    context.requireWait = false;
                    context.require(cfg.priority);
                    //Trigger a resume right away, for the case when
                    //the script with the priority load is done as part
                    //of a data-main call. In that case the normal resume
                    //call will not happen because the scriptCount will be
                    //at 1, since the script for data-main is being processed.
                    resume();
                    //Restore previous state.
                    context.requireWait = requireWait;
                    config.priorityWait = cfg.priority;
                }

                //If a deps array or a config callback is specified, then call
                //require with those args. This is useful when require is defined as a
                //config object before require.js is loaded.
                if (cfg.deps || cfg.callback) {
                    context.require(cfg.deps || [], cfg.callback);
                }

                //Set up ready callback, if asked. Useful when require is defined as a
                //config object before require.js is loaded.
                if (cfg.ready) {
                    req.ready(cfg.ready);
                }
            },

            isDefined: function (moduleName, relModuleMap) {
                return makeModuleMap(moduleName, relModuleMap).fullName in defined;
            },

            require: function (deps, callback, relModuleMap) {
                var moduleName, ret, moduleMap;
                if (typeof deps === "string") {
                    //Synchronous access to one module. If require.get is
                    //available (as in the Node adapter), prefer that.
                    //In this case deps is the moduleName and callback is
                    //the relModuleMap
                    if (req.get) {
                        return req.get(context, deps, callback);
                    }

                    //Just return the module wanted. In this scenario, the
                    //second arg (if passed) is just the relModuleMap.
                    moduleName = deps;
                    relModuleMap = callback;

                    //Normalize module name, if it contains . or ..
                    moduleMap = makeModuleMap(moduleName, relModuleMap);

                    ret = defined[moduleMap.fullName];
                    if (ret === undefined) {
                        return req.onError(new Error("require: module name '" +
                                    moduleMap.fullName +
                                    "' has not been loaded yet for context: " +
                                    contextName));
                    }
                    return ret;
                }

                main(null, deps, callback, relModuleMap);

                //If the require call does not trigger anything new to load,
                //then resume the dependency processing.
                if (!context.requireWait) {
                    while (!context.scriptCount && context.paused.length) {
                        resume();
                    }
                }
                return undefined;
            },

            /**
             * Internal method to transfer globalQueue items to this context's
             * defQueue.
             */
            takeGlobalQueue: function () {
                //Push all the globalDefQueue items into the context's defQueue
                if (globalDefQueue.length) {
                    //Array splice in the values since the context code has a
                    //local var ref to defQueue, so cannot just reassign the one
                    //on context.
                    apsp.apply(context.defQueue,
                               [context.defQueue.length - 1, 0].concat(globalDefQueue));
                    globalDefQueue = [];
                }
            },

            /**
             * Internal method used by environment adapters to complete a load event.
             * A load event could be a script load or just a load pass from a synchronous
             * load call.
             * @param {String} moduleName the name of the module to potentially complete.
             */
            completeLoad: function (moduleName) {
                var args;

                context.takeGlobalQueue();

                while (defQueue.length) {
                    args = defQueue.shift();

                    if (args[0] === null) {
                        args[0] = moduleName;
                        break;
                    } else if (args[0] === moduleName) {
                        //Found matching require.def call for this script!
                        break;
                    } else {
                        //Some other named require.def call, most likely the result
                        //of a build layer that included many require.def calls.
                        callDefMain(args);
                        args = null;
                    }
                }
                if (args) {
                    callDefMain(args);
                } else {
                    //A script that does not call define(), so just simulate
                    //the call for it. Special exception for jQuery dynamic load.
                    callDefMain([moduleName, [],
                                moduleName === "jquery" && typeof jQuery !== "undefined" ?
                                function () {
                                    return jQuery;
                                } : null]);
                }

                //Mark the script as loaded. Note that this can be different from a
                //moduleName that maps to a require.def call. This line is important
                //for traditional browser scripts.
                loaded[moduleName] = true;

                //If a global jQuery is defined, check for it. Need to do it here
                //instead of main() since stock jQuery does not register as
                //a module via define.
                jQueryCheck();

                //Doing this scriptCount decrement branching because sync envs
                //need to decrement after resume, otherwise it looks like
                //loading is complete after the first dependency is fetched.
                //For browsers, it works fine to decrement after, but it means
                //the checkLoaded setTimeout 50 ms cost is taken. To avoid
                //that cost, decrement beforehand.
                if (req.isAsync) {
                    context.scriptCount -= 1;
                }
                resume();
                if (!req.isAsync) {
                    context.scriptCount -= 1;
                }
            },

            /**
             * Converts a module name + .extension into an URL path.
             * *Requires* the use of a module name. It does not support using
             * plain URLs like nameToUrl.
             */
            toUrl: function (moduleNamePlusExt, relModuleMap) {
                var index = moduleNamePlusExt.lastIndexOf("."),
                    ext = null;

                if (index !== -1) {
                    ext = moduleNamePlusExt.substring(index, moduleNamePlusExt.length);
                    moduleNamePlusExt = moduleNamePlusExt.substring(0, index);
                }

                return context.nameToUrl(moduleNamePlusExt, ext, relModuleMap);
            },

            /**
             * Converts a module name to a file path. Supports cases where
             * moduleName may actually be just an URL.
             */
            nameToUrl: function (moduleName, ext, relModuleMap) {
                var paths, pkgs, pkg, pkgPath, syms, i, parentModule, url,
                    config = context.config;

                if (moduleName.indexOf("./") === 0 || moduleName.indexOf("../") === 0) {
                    //A relative ID, just map it relative to relModuleMap's url
                    syms = relModuleMap && relModuleMap.url ? relModuleMap.url.split('/') : [];
                    //Pop off the file name.
                    if (syms.length) {
                        syms.pop();
                    }
                    syms = syms.concat(moduleName.split('/'));
                    trimDots(syms);
                    url = syms.join('/') +
                          (ext ? ext :
                          (req.jsExtRegExp.test(moduleName) ? "" : ".js"));
                } else {

                    //Normalize module name if have a base relative module name to work from.
                    moduleName = normalize(moduleName, relModuleMap);

                    //If a colon is in the URL, it indicates a protocol is used and it is just
                    //an URL to a file, or if it starts with a slash or ends with .js, it is just a plain file.
                    //The slash is important for protocol-less URLs as well as full paths.
                    if (req.jsExtRegExp.test(moduleName)) {
                        //Just a plain path, not module name lookup, so just return it.
                        //Add extension if it is included. This is a bit wonky, only non-.js things pass
                        //an extension, this method probably needs to be reworked.
                        url = moduleName + (ext ? ext : "");
                    } else {
                        //A module that needs to be converted to a path.
                        paths = config.paths;
                        pkgs = config.pkgs;

                        syms = moduleName.split("/");
                        //For each module name segment, see if there is a path
                        //registered for it. Start with most specific name
                        //and work up from it.
                        for (i = syms.length; i > 0; i--) {
                            parentModule = syms.slice(0, i).join("/");
                            if (paths[parentModule]) {
                                syms.splice(0, i, paths[parentModule]);
                                break;
                            } else if ((pkg = pkgs[parentModule])) {
                                //If module name is just the package name, then looking
                                //for the main module.
                                if (moduleName === pkg.name) {
                                    pkgPath = pkg.location + '/' + pkg.main;
                                } else {
                                    pkgPath = pkg.location + '/' + pkg.lib;
                                }
                                syms.splice(0, i, pkgPath);
                                break;
                            }
                        }

                        //Join the path parts together, then figure out if baseUrl is needed.
                        url = syms.join("/") + (ext || ".js");
                        url = (url.charAt(0) === '/' || url.match(/^\w+:/) ? "" : config.baseUrl) + url;
                    }
                }

                return config.urlArgs ? url +
                                        ((url.indexOf('?') === -1 ? '?' : '&') +
                                         config.urlArgs) : url;
            }
        };

        //Make these visible on the context so can be called at the very
        //end of the file to bootstrap
        context.jQueryCheck = jQueryCheck;
        context.resume = resume;

        return context;
    }

    /**
     * Main entry point.
     *
     * If the only argument to require is a string, then the module that
     * is represented by that string is fetched for the appropriate context.
     *
     * If the first argument is an array, then it will be treated as an array
     * of dependency string names to fetch. An optional function callback can
     * be specified to execute when all of those dependencies are available.
     *
     * Make a local req variable to help Caja compliance (it assumes things
     * on a require that are not standardized), and to give a short
     * name for minification/local scope use.
     */
    req = require = function (deps, callback) {

        //Find the right context, use default
        var contextName = defContextName,
            context, config;

        // Determine if have config object in the call.
        if (!isArray(deps) && typeof deps !== "string") {
            // deps is a config object
            config = deps;
            if (isArray(callback)) {
                // Adjust args if there are dependencies
                deps = callback;
                callback = arguments[2];
            } else {
                deps = [];
            }
        }

        if (config && config.context) {
            contextName = config.context;
        }

        context = contexts[contextName] ||
                  (contexts[contextName] = newContext(contextName));

        if (config) {
            context.configure(config);
        }

        return context.require(deps, callback);
    };

    req.version = version;
    req.isArray = isArray;
    req.isFunction = isFunction;
    req.mixin = mixin;
    //Used to filter out dependencies that are already paths.
    req.jsExtRegExp = /^\/|:|\?|\.js$/;
    s = req.s = {
        contexts: contexts,
        //Stores a list of URLs that should not get async script tag treatment.
        skipAsync: {},
        isPageLoaded: !isBrowser,
        readyCalls: []
    };

    req.isAsync = req.isBrowser = isBrowser;
    if (isBrowser) {
        head = s.head = document.getElementsByTagName("head")[0];
        //If BASE tag is in play, using appendChild is a problem for IE6.
        //When that browser dies, this can be removed. Details in this jQuery bug:
        //http://dev.jquery.com/ticket/2709
        baseElement = document.getElementsByTagName("base")[0];
        if (baseElement) {
            head = s.head = baseElement.parentNode;
        }
    }

    /**
     * Any errors that require explicitly generates will be passed to this
     * function. Intercept/override it if you want custom error handling.
     * @param {Error} err the error object.
     */
    req.onError = function (err) {
        throw err;
    };

    /**
     * Does the request to load a module for the browser case.
     * Make this a separate function to allow other environments
     * to override it.
     *
     * @param {Object} context the require context to find state.
     * @param {String} moduleName the name of the module.
     * @param {Object} url the URL to the module.
     */
    req.load = function (context, moduleName, url) {
        var contextName = context.contextName,
            urlFetched = context.urlFetched,
            loaded = context.loaded;
        isDone = false;

        //Only set loaded to false for tracking if it has not already been set.
        if (!loaded[moduleName]) {
            loaded[moduleName] = false;
        }

        if (!urlFetched[url]) {
            context.scriptCount += 1;
            req.attach(url, contextName, moduleName);
            urlFetched[url] = true;

            //If tracking a jQuery, then make sure its readyWait
            //is incremented to prevent its ready callbacks from
            //triggering too soon.
            if (context.jQuery && !context.jQueryIncremented) {
                context.jQuery.readyWait += 1;
                context.jQueryIncremented = true;
            }
        }
    };

    function getInteractiveScript() {
        var scripts, i, script;
        if (interactiveScript && interactiveScript.readyState === 'interactive') {
            return interactiveScript;
        }

        scripts = document.getElementsByTagName('script');
        for (i = scripts.length - 1; i > -1 && (script = scripts[i]); i--) {
            if (script.readyState === 'interactive') {
                return (interactiveScript = script);
            }
        }

        return null;
    }

    /**
     * The function that handles definitions of modules. Differs from
     * require() in that a string for the module should be the first argument,
     * and the function to execute after dependencies are loaded should
     * return a value to define the module corresponding to the first argument's
     * name.
     */
    define = req.def = function (name, deps, callback) {
        var node, context;

        //Allow for anonymous functions
        if (typeof name !== 'string') {
            //Adjust args appropriately
            callback = deps;
            deps = name;
            name = null;
        }

        //This module may not have dependencies
        if (!req.isArray(deps)) {
            callback = deps;
            deps = [];
        }

        //If no name, and callback is a function, then figure out if it a
        //CommonJS thing with dependencies.
        if (!name && !deps.length && req.isFunction(callback)) {
            //Remove comments from the callback string,
            //look for require calls, and pull them into the dependencies,
            //but only if there are function args.
            if (callback.length) {
                callback
                    .toString()
                    .replace(commentRegExp, "")
                    .replace(cjsRequireRegExp, function (match, dep) {
                        deps.push(dep);
                    });

                //May be a CommonJS thing even without require calls, but still
                //could use exports, and such, so always add those as dependencies.
                //This is a bit wasteful for RequireJS modules that do not need
                //an exports or module object, but erring on side of safety.
                //REQUIRES the function to expect the CommonJS variables in the
                //order listed below.
                deps = ["require", "exports", "module"].concat(deps);
            }
        }

        //If in IE 6-8 and hit an anonymous define() call, do the interactive
        //work.
        if (useInteractive) {
            node = currentlyAddingScript || getInteractiveScript();
            if (!node) {
                return req.onError(new Error("ERROR: No matching script interactive for " + callback));
            }
            if (!name) {
                name = node.getAttribute("data-requiremodule");
            }
            context = contexts[node.getAttribute("data-requirecontext")];
        }

        //Always save off evaluating the def call until the script onload handler.
        //This allows multiple modules to be in a file without prematurely
        //tracing dependencies, and allows for anonymous module support,
        //where the module name is not known until the script onload event
        //occurs. If no context, use the global queue, and get it processed
        //in the onscript load callback.
        (context ? context.defQueue : globalDefQueue).push([name, deps, callback]);

        return undefined;
    };

    define.amd = {
        multiversion: true,
        plugins: true
    };

    /**
     * Executes a module callack function. Broken out as a separate function
     * solely to allow the build system to sequence the files in the built
     * layer in the right sequence.
     *
     * @private
     */
    req.execCb = function (name, callback, args) {
        return callback.apply(null, args);
    };

    /**
     * callback for script loads, used to check status of loading.
     *
     * @param {Event} evt the event from the browser for the script
     * that was loaded.
     *
     * @private
     */
    req.onScriptLoad = function (evt) {
        //Using currentTarget instead of target for Firefox 2.0's sake. Not
        //all old browsers will be supported, but this one was easy enough
        //to support and still makes sense.
        var node = evt.currentTarget || evt.srcElement, contextName, moduleName,
            context;

        if (evt.type === "load" || readyRegExp.test(node.readyState)) {
            //Reset interactive script so a script node is not held onto for
            //to long.
            interactiveScript = null;

            //Pull out the name of the module and the context.
            contextName = node.getAttribute("data-requirecontext");
            moduleName = node.getAttribute("data-requiremodule");
            context = contexts[contextName];

            contexts[contextName].completeLoad(moduleName);

            //Clean up script binding.
            if (node.removeEventListener) {
                node.removeEventListener("load", req.onScriptLoad, false);
            } else {
                //Probably IE. If not it will throw an error, which will be
                //useful to know.
                node.detachEvent("onreadystatechange", req.onScriptLoad);
            }
        }
    };

    /**
     * Attaches the script represented by the URL to the current
     * environment. Right now only supports browser loading,
     * but can be redefined in other environments to do the right thing.
     * @param {String} url the url of the script to attach.
     * @param {String} contextName the name of the context that wants the script.
     * @param {moduleName} the name of the module that is associated with the script.
     * @param {Function} [callback] optional callback, defaults to require.onScriptLoad
     * @param {String} [type] optional type, defaults to text/javascript
     */
    req.attach = function (url, contextName, moduleName, callback, type) {
        var node, loaded, context;
        if (isBrowser) {
            //In the browser so use a script tag
            callback = callback || req.onScriptLoad;
            node = document.createElement("script");
            node.type = type || "text/javascript";
            node.charset = "utf-8";
            //Use async so Gecko does not block on executing the script if something
            //like a long-polling comet tag is being run first. Gecko likes
            //to evaluate scripts in DOM order, even for dynamic scripts.
            //It will fetch them async, but only evaluate the contents in DOM
            //order, so a long-polling script tag can delay execution of scripts
            //after it. But telling Gecko we expect async gets us the behavior
            //we want -- execute it whenever it is finished downloading. Only
            //Helps Firefox 3.6+
            //Allow some URLs to not be fetched async. Mostly helps the order!
            //plugin
            node.async = !s.skipAsync[url];

            node.setAttribute("data-requirecontext", contextName);
            node.setAttribute("data-requiremodule", moduleName);

            //Set up load listener.
            if (node.addEventListener) {
                node.addEventListener("load", callback, false);
            } else {
                //Probably IE. If not it will throw an error, which will be
                //useful to know. IE (at least 6-8) do not fire
                //script onload right after executing the script, so
                //we cannot tie the anonymous require.def call to a name.
                //However, IE reports the script as being in "interactive"
                //readyState at the time of the require.def call.
                useInteractive = true;
                node.attachEvent("onreadystatechange", callback);
            }
            node.src = url;

            //For some cache cases in IE 6-8, the script executes before the end
            //of the appendChild execution, so to tie an anonymous require.def
            //call to the module name (which is stored on the node), hold on
            //to a reference to this node, but clear after the DOM insertion.
            currentlyAddingScript = node;
            if (baseElement) {
                head.insertBefore(node, baseElement);
            } else {
                head.appendChild(node);
            }
            currentlyAddingScript = null;
            return node;
        } else if (isWebWorker) {
            //In a web worker, use importScripts. This is not a very
            //efficient use of importScripts, importScripts will block until
            //its script is downloaded and evaluated. However, if web workers
            //are in play, the expectation that a build has been done so that
            //only one script needs to be loaded anyway. This may need to be
            //reevaluated if other use cases become common.
            context = contexts[contextName];
            loaded = context.loaded;
            loaded[moduleName] = false;

            importScripts(url);

            //Account for anonymous modules
            context.completeLoad(moduleName);
        }
        return null;
    };

    //Look for a data-main script attribute, which could also adjust the baseUrl.
    if (isBrowser) {
        //Figure out baseUrl. Get it from the script tag with require.js in it.
        scripts = document.getElementsByTagName("script");

        for (i = scripts.length - 1; i > -1 && (script = scripts[i]); i--) {
            //Set the "head" where we can append children by
            //using the script's parent.
            if (!head) {
                head = script.parentNode;
            }

            //Look for a data-main attribute to set main script for the page
            //to load. If it is there, the path to data main becomes the
            //baseUrl, if it is not already set.
            if ((dataMain = script.getAttribute('data-main'))) {
                if (!cfg.baseUrl) {
                    //Pull off the directory of data-main for use as the
                    //baseUrl.
                    src = dataMain.split('/');
                    mainScript = src.pop();
                    subPath = src.length ? src.join('/')  + '/' : './';

                    //Set final config.
                    cfg.baseUrl = subPath;
                    //Strip off any trailing .js since dataMain is now
                    //like a module name.
                    dataMain = mainScript.replace(jsSuffixRegExp, '');
                }

                //Put the data-main script in the files to load.
                cfg.deps = cfg.deps ? cfg.deps.concat(dataMain) : [dataMain];

                break;
            }
        }
    }

    //Set baseUrl based on config.
    s.baseUrl = cfg.baseUrl;

    //****** START page load functionality ****************
    /**
     * Sets the page as loaded and triggers check for all modules loaded.
     */
    req.pageLoaded = function () {
        if (!s.isPageLoaded) {
            s.isPageLoaded = true;
            if (scrollIntervalId) {
                clearInterval(scrollIntervalId);
            }

            //Part of a fix for FF < 3.6 where readyState was not set to
            //complete so libraries like jQuery that check for readyState
            //after page load where not getting initialized correctly.
            //Original approach suggested by Andrea Giammarchi:
            //http://webreflection.blogspot.com/2009/11/195-chars-to-help-lazy-loading.html
            //see other setReadyState reference for the rest of the fix.
            if (setReadyState) {
                document.readyState = "complete";
            }

            req.callReady();
        }
    };

    //See if there is nothing waiting across contexts, and if not, trigger
    //callReady.
    req.checkReadyState = function () {
        var contexts = s.contexts, prop;
        for (prop in contexts) {
            if (!(prop in empty)) {
                if (contexts[prop].waitCount) {
                    return;
                }
            }
        }
        s.isDone = true;
        req.callReady();
    };

    /**
     * Internal function that calls back any ready functions. If you are
     * integrating RequireJS with another library without require.ready support,
     * you can define this method to call your page ready code instead.
     */
    req.callReady = function () {
        var callbacks = s.readyCalls, i, callback, contexts, context, prop;

        if (s.isPageLoaded && s.isDone) {
            if (callbacks.length) {
                s.readyCalls = [];
                for (i = 0; (callback = callbacks[i]); i++) {
                    callback();
                }
            }

            //If jQuery with readyWait is being tracked, updated its
            //readyWait count.
            contexts = s.contexts;
            for (prop in contexts) {
                if (!(prop in empty)) {
                    context = contexts[prop];
                    if (context.jQueryIncremented) {
                        context.jQuery.ready(true);
                        context.jQueryIncremented = false;
                    }
                }
            }
        }
    };

    /**
     * Registers functions to call when the page is loaded
     */
    req.ready = function (callback) {
        if (s.isPageLoaded && s.isDone) {
            callback();
        } else {
            s.readyCalls.push(callback);
        }
        return req;
    };

    if (isBrowser) {
        if (document.addEventListener) {
            //Standards. Hooray! Assumption here that if standards based,
            //it knows about DOMContentLoaded.
            document.addEventListener("DOMContentLoaded", req.pageLoaded, false);
            window.addEventListener("load", req.pageLoaded, false);
            //Part of FF < 3.6 readystate fix (see setReadyState refs for more info)
            if (!document.readyState) {
                setReadyState = true;
                document.readyState = "loading";
            }
        } else if (window.attachEvent) {
            window.attachEvent("onload", req.pageLoaded);

            //DOMContentLoaded approximation, as found by Diego Perini:
            //http://javascript.nwbox.com/IEContentLoaded/
            if (self === self.top) {
                scrollIntervalId = setInterval(function () {
                    try {
                        //From this ticket:
                        //http://bugs.dojotoolkit.org/ticket/11106,
                        //In IE HTML Application (HTA), such as in a selenium test,
                        //javascript in the iframe can't see anything outside
                        //of it, so self===self.top is true, but the iframe is
                        //not the top window and doScroll will be available
                        //before document.body is set. Test document.body
                        //before trying the doScroll trick.
                        if (document.body) {
                            document.documentElement.doScroll("left");
                            req.pageLoaded();
                        }
                    } catch (e) {}
                }, 30);
            }
        }

        //Check if document already complete, and if so, just trigger page load
        //listeners. NOTE: does not work with Firefox before 3.6. To support
        //those browsers, manually call require.pageLoaded().
        if (document.readyState === "complete") {
            req.pageLoaded();
        }
    }
    //****** END page load functionality ****************

    //Set up default context. If require was a configuration object, use that as base config.
    req(cfg);

    //If modules are built into require.js, then need to make sure dependencies are
    //traced. Use a setTimeout in the browser world, to allow all the modules to register
    //themselves. In a non-browser env, assume that modules are not built into require.js,
    //which seems odd to do on the server.
    if (req.isAsync && typeof setTimeout !== "undefined") {
        ctx = s.contexts[(cfg.context || defContextName)];
        //Indicate that the script that includes require() is still loading,
        //so that require()'d dependencies are not traced until the end of the
        //file is parsed (approximated via the setTimeout call).
        ctx.requireWait = true;
        setTimeout(function () {
            ctx.requireWait = false;

            //Any modules included with the require.js file will be in the
            //global queue, assign them to this context.
            ctx.takeGlobalQueue();

            //Allow for jQuery to be loaded/already in the page, and if jQuery 1.4.3,
            //make sure to hold onto it for readyWait triggering.
            ctx.jQueryCheck();

            if (!ctx.scriptCount) {
                ctx.resume();
            }
            req.checkReadyState();
        }, 0);
    }
}());

/**
 * @license RequireJS i18n Copyright (c) 2010-2011, The Dojo Foundation All Rights Reserved.
 * Available via the MIT or new BSD license.
 * see: http://github.com/jrburke/requirejs for details
 */
/*jslint white: false, regexp: false, nomen: false, plusplus: false, strict: false */
/*global require: false, navigator: false, define: false */

/**
 * This plugin handles i18n! prefixed modules. It does the following:
 *
 * 1) A regular module can have a dependency on an i18n bundle, but the regular
 * module does not want to specify what locale to load. So it just specifies
 * the top-level bundle, like "i18n!nls/colors".
 *
 * This plugin will load the i18n bundle at nls/colors, see that it is a root/master
 * bundle since it does not have a locale in its name. It will then try to find
 * the best match locale available in that master bundle, then request all the
 * locale pieces for that best match locale. For instance, if the locale is "en-us",
 * then the plugin will ask for the "en-us", "en" and "root" bundles to be loaded
 * (but only if they are specified on the master bundle).
 *
 * Once all the bundles for the locale pieces load, then it mixes in all those
 * locale pieces into each other, then finally sets the context.defined value
 * for the nls/colors bundle to be that mixed in locale.
 *
 * 2) A regular module specifies a specific locale to load. For instance,
 * i18n!nls/fr-fr/colors. In this case, the plugin needs to load the master bundle
 * first, at nls/colors, then figure out what the best match locale is for fr-fr,
 * since maybe only fr or just root is defined for that locale. Once that best
 * fit is found, all of its locale pieces need to have their bundles loaded.
 *
 * Once all the bundles for the locale pieces load, then it mixes in all those
 * locale pieces into each other, then finally sets the context.defined value
 * for the nls/fr-fr/colors bundle to be that mixed in locale.
 */
(function () {
    //regexp for reconstructing the master bundle name from parts of the regexp match
    //nlsRegExp.exec("foo/bar/baz/nls/en-ca/foo") gives:
    //["foo/bar/baz/nls/en-ca/foo", "foo/bar/baz/nls/", "/", "/", "en-ca", "foo"]
    //nlsRegExp.exec("foo/bar/baz/nls/foo") gives:
    //["foo/bar/baz/nls/foo", "foo/bar/baz/nls/", "/", "/", "foo", ""]
    //so, if match[5] is blank, it means this is the top bundle definition.
    var nlsRegExp = /(^.*(^|\/)nls(\/|$))([^\/]*)\/?([^\/]*)/;

    //Helper function to avoid repeating code. Lots of arguments in the
    //desire to stay functional and support RequireJS contexts without having
    //to know about the RequireJS contexts.
    function addPart(locale, master, needed, toLoad, prefix, suffix) {
        if (master[locale]) {
            needed.push(locale);
            if (master[locale] === true || master[locale] === 1) {
                toLoad.push(prefix + locale + '/' + suffix);
            }
        }
    }

    function addIfExists(req, locale, toLoad, prefix, suffix) {
        var fullName = prefix + locale + '/' + suffix;
        if (require._fileExists(req.nameToUrl(fullName, null))) {
            toLoad.push(fullName);
        }
    }

    define('require/i18n',{
        /**
         * Called when a dependency needs to be loaded.
         */
        load: function (name, req, onLoad, config) {
            config = config || {};

            var masterName,
                match = nlsRegExp.exec(name),
                prefix = match[1],
                locale = match[4],
                suffix = match[5],
                parts = locale.split("-"),
                toLoad = [],
                value = {},
                i, part, current = "";

            //If match[5] is blank, it means this is the top bundle definition,
            //so it does not have to be handled. Locale-specific requests
            //will have a match[4] value but no match[5]
            if (match[5]) {
                //locale-specific bundle
                prefix = match[1];
                masterName = prefix + suffix;
            } else {
                //Top-level bundle.
                masterName = name;
                suffix = match[4];
                locale = config.locale || (config.locale =
                        typeof navigator === "undefined" ? "root" :
                        (navigator.language ||
                         navigator.userLanguage || "root").toLowerCase());
                parts = locale.split("-");
            }

            if (config.isBuild) {
                //Check for existence of all locale possible files and
                //require them if exist.
                toLoad.push(masterName);
                addIfExists(req, "root", toLoad, prefix, suffix);
                for (i = 0; (part = parts[i]); i++) {
                    current += (current ? "-" : "") + part;
                    addIfExists(req, current, toLoad, prefix, suffix);
                }
                req(toLoad);
                onLoad();
            } else {
                //First, fetch the master bundle, it knows what locales are available.
                req([masterName], function (master) {
                    //Figure out the best fit
                    var needed = [];

                    //Always allow for root, then do the rest of the locale parts.
                    addPart("root", master, needed, toLoad, prefix, suffix);
                    for (i = 0; (part = parts[i]); i++) {
                        current += (current ? "-" : "") + part;
                        addPart(current, master, needed, toLoad, prefix, suffix);
                    }

                    //Load all the parts missing.
                    req(toLoad, function () {
                        var i, partBundle;
                        for (i = needed.length - 1; i > -1 && (part = needed[i]); i--) {
                            partBundle = master[part];
                            if (partBundle === true || partBundle === 1) {
                                partBundle = req(prefix + part + '/' + suffix);
                            }
                            require.mixin(value, partBundle);
                        }

                        //All done, notify the loader.
                        onLoad(value);
                    });
                });
            }
        }
    });
}());
/**
 * @license RequireJS text Copyright (c) 2010-2011, The Dojo Foundation All Rights Reserved.
 * Available via the MIT or new BSD license.
 * see: http://github.com/jrburke/requirejs for details
 */
/*jslint white: false, regexp: false, nomen: false, plusplus: false, strict: false */
/*global require: false, XMLHttpRequest: false, ActiveXObject: false,
  define: false */

(function () {
    var progIds = ['Msxml2.XMLHTTP', 'Microsoft.XMLHTTP', 'Msxml2.XMLHTTP.4.0'],
        xmlRegExp = /^\s*<\?xml(\s)+version=[\'\"](\d)*.(\d)*[\'\"](\s)*\?>/im,
        bodyRegExp = /<body[^>]*>\s*([\s\S]+)\s*<\/body>/im,
        buildMap = [];

    if (!require.textStrip) {
        require.textStrip = function (text) {
            //Strips <?xml ...?> declarations so that external SVG and XML
            //documents can be added to a document without worry. Also, if the string
            //is an HTML document, only the part inside the body tag is returned.
            if (text) {
                text = text.replace(xmlRegExp, "");
                var matches = text.match(bodyRegExp);
                if (matches) {
                    text = matches[1];
                }
            } else {
                text = "";
            }
            return text;
        };
    }

    if (!require.jsEscape) {
        require.jsEscape = function (text) {
            return text.replace(/(['\\])/g, '\\$1')
                .replace(/[\f]/g, "\\f")
                .replace(/[\b]/g, "\\b")
                .replace(/[\n]/g, "\\n")
                .replace(/[\t]/g, "\\t")
                .replace(/[\r]/g, "\\r");
        };
    }

    //Upgrade require to add some methods for XHR handling. But it could be that
    //this require is used in a non-browser env, so detect for existing method
    //before attaching one.
    if (!require.getXhr) {
        require.getXhr = function () {
            //Would love to dump the ActiveX crap in here. Need IE 6 to die first.
            var xhr, i, progId;
            if (typeof XMLHttpRequest !== "undefined") {
                return new XMLHttpRequest();
            } else {
                for (i = 0; i < 3; i++) {
                    progId = progIds[i];
                    try {
                        xhr = new ActiveXObject(progId);
                    } catch (e) {}

                    if (xhr) {
                        progIds = [progId];  // so faster next time
                        break;
                    }
                }
            }

            if (!xhr) {
                throw new Error("require.getXhr(): XMLHttpRequest not available");
            }

            return xhr;
        };
    }

    if (!require.fetchText) {
        require.fetchText = function (url, callback) {
            var xhr = require.getXhr();
            xhr.open('GET', url, true);
            xhr.onreadystatechange = function (evt) {
                //Do not explicitly handle errors, those should be
                //visible via console output in the browser.
                if (xhr.readyState === 4) {
                    callback(xhr.responseText);
                }
            };
            xhr.send(null);
        };
    }

    define('require/text',['require','exports','module'],function () {
        return {
            load: function (name, req, onLoad, config) {
                //Name has format: some.module.filext!strip
                //The strip part is optional.
                //if strip is present, then that means only get the string contents
                //inside a body tag in an HTML string. For XML/SVG content it means
                //removing the <?xml ...?> declarations so the content can be inserted
                //into the current doc without problems.

                var strip = false, url, index = name.indexOf("."),
                    modName = name.substring(0, index),
                    ext = name.substring(index + 1, name.length);

                index = ext.indexOf("!");
                if (index !== -1) {
                    //Pull off the strip arg.
                    strip = ext.substring(index + 1, ext.length);
                    strip = strip === "strip";
                    ext = ext.substring(0, index);
                }

                //Load the text.
                url = req.nameToUrl(modName, "." + ext);
                require.fetchText(url, function (text) {
                    text = strip ? require.textStrip(text) : text;
                    if (config.isBuild && config.inlineText) {
                        buildMap[name] = text;
                    }
                    onLoad(text);
                });
            },

            write: function (pluginName, moduleName, write) {
                if (moduleName in buildMap) {
                    var text = require.jsEscape(buildMap[moduleName]);
                    write("define('" + pluginName + "!" + moduleName  +
                          "', function () { return '" + text + "';});\n");
                }
            }
        };
    });
}());
/**
 * @license RequireJS order Copyright (c) 2010-2011, The Dojo Foundation All Rights Reserved.
 * Available via the MIT or new BSD license.
 * see: http://github.com/jrburke/requirejs for details
 */
/*jslint white: false, nomen: false, plusplus: false, strict: false */
/*global require: false, define: false, window: false, document: false,
  setTimeout: false */

(function () {
    //Sadly necessary browser inference due to differences in the way
    //that browsers load and execute dynamically inserted javascript
    //and whether the script/cache method works.
    //Currently, Gecko and Opera do not load/fire onload for scripts with
    //type="script/cache" but they execute injected scripts in order
    //unless the 'async' flag is present.
    //However, this is all changing in latest browsers implementing HTML5
    //spec. Firefox nightly supports using the .async true by default, and
    //if false, then it will execute in order. Favor that test first for forward
    //compatibility. However, it is unclear if webkit/IE will follow suit.
    //Latest webkit breaks the script/cache trick.
    //Test for document and window so that this file can be loaded in
    //a web worker/non-browser env. It will not make sense to use this
    //plugin in a non-browser env, but the file should not error out if included
    //in the allplugins-require.js file, then loaded in a non-browser env.
    var supportsInOrderExecution = typeof document !== "undefined" &&
                                   typeof window !== "undefined" &&
                                   (document.createElement("script").async ||
                               (window.opera && Object.prototype.toString.call(window.opera) === "[object Opera]") ||
                               //If Firefox 2 does not have to be supported, then
                               //a better check may be:
                               //('mozIsLocallyAvailable' in window.navigator)
                               ("MozAppearance" in document.documentElement.style)),
        readyRegExp = /^(complete|loaded)$/,
        waiting = [],
        cached = {};

    function loadResource(name, req, onLoad) {
        req([name], function (value) {
            //The value may be a real defined module. Wrap
            //it in a function call, because this function is used
            //as the factory function for this ordered dependency.
            onLoad(function () {
                return value;
            });
        });
    }

    //Callback used by the type="script/cache" callback that indicates a script
    //has finished downloading.
    function scriptCacheCallback(evt) {
        var node = evt.currentTarget || evt.srcElement, i,
            moduleName, resource;

        if (evt.type === "load" || readyRegExp.test(node.readyState)) {
            //Pull out the name of the module and the context.
            moduleName = node.getAttribute("data-requiremodule");

            //Mark this cache request as loaded
            cached[moduleName] = true;

            //Find out how many ordered modules have loaded
            for (i = 0; (resource = waiting[i]); i++) {
                if (cached[resource.name]) {
                    loadResource(resource.name, resource.req, resource.onLoad);
                } else {
                    //Something in the ordered list is not loaded,
                    //so wait.
                    break;
                }
            }

            //If just loaded some items, remove them from waiting.
            if (i > 0) {
                waiting.splice(0, i);
            }

            //Remove this script tag from the DOM
            //Use a setTimeout for cleanup because some older IE versions vomit
            //if removing a script node while it is being evaluated.
            setTimeout(function () {
                node.parentNode.removeChild(node);
            }, 15);
        }
    }

    define('require/order',{
        load: function (name, req, onLoad, config) {
            var url = req.nameToUrl(name, null);

            //If a build, just load the module as usual.
            if (config.isBuild) {
                loadResource(name, req, onLoad);
                return;
            }

            //Make sure the async attribute is not set for any pathway involving
            //this script.
            require.s.skipAsync[url] = true;
            if (supportsInOrderExecution) {
                //Just a normal script tag append, but without async attribute
                //on the script.
                req([name], function (value) {
                    //The value may be a real defined module. Wrap
                    //it in a function call, because this function is used
                    //as the factory function for this ordered dependency.
                    onLoad(function () {
                        return value;
                    });
                });
            } else {
                //Credit to LABjs author Kyle Simpson for finding that scripts
                //with type="script/cache" allow scripts to be downloaded into
                //browser cache but not executed. Use that
                //so that subsequent addition of a real type="text/javascript"
                //tag will cause the scripts to be executed immediately in the
                //correct order.
                if (req.isDefined(name)) {
                    req([name], function (value) {
                        //The value may be a real defined module. Wrap
                        //it in a function call, because this function is used
                        //as the factory function for this ordered dependency.
                        onLoad(function () {
                            return value;
                        });
                    });
                } else {
                    waiting.push({
                        name: name,
                        req: req,
                        onLoad: onLoad
                    });
                    require.attach(url, "", name, scriptCacheCallback, "script/cache");
                }
            }
        }
    });
}());

