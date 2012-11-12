/**
 * @license RequireJS order 0.26.0 Copyright (c) 2010-2011, The Dojo Foundation All Rights Reserved.
 * Available via the MIT or new BSD license.
 * see: http://github.com/jrburke/requirejs for details
 */
/*jslint nomen: false, plusplus: false, strict: false */
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
    //in a file, then loaded in a non-browser env.
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

    define({
        version: '0.26.0',

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
                if (req.specified(name)) {
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
                    require.attach(url, null, name, scriptCacheCallback, "script/cache");
                }
            }
        }
    });
}());

