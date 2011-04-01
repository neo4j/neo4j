has = (function(g){

    // summary: A simple feature detection function/framework.
    //
    // name: String
    //      The name of the feature to detect, as defined by the overall `has` tests.
    //      Tests can be registered via `has.add(testname, testfunction)`.
    //
    // example:
    //      mylibrary.bind = has("native-bind") ? function(fn, context){
    //          return fn.bind(context);
    //      } : function(fn, context){
    //          return function(){
    //              fn.apply(context, arguments);
    //          }
    //      }

    var NON_HOST_TYPES = { "boolean": 1, "number": 1, "string": 1, "undefined": 1 },
        VENDOR_PREFIXES = ["Webkit", "Moz", "O", "ms", "Khtml"],
        d = isHostType(g, "document") && g.document,
        el = d && isHostType(d, "createElement") && d.createElement("DiV"),
        testCache = {}
    ;

    function has(/* String */name){
        if(typeof testCache[name] == "function"){
            testCache[name] = testCache[name](g, d, el);
        }
        return testCache[name]; // Boolean
    }

    function add(/* String */name, /* Function */test, /* Boolean? */now){
        // summary: Register a new feature detection test for some named feature
        //
        // name: String
        //      The name of the feature to test.
        //
        // test: Function
        //      A test function to register. If a function, queued for testing until actually
        //      needed. The test function should return a boolean indicating
        //      the presence of a feature or bug.
        //
        // now: Boolean?
        //      Optional. Omit if `test` is not a function. Provides a way to immediately
        //      run the test and cache the result.
        // example:
        //      A redundant test, testFn with immediate execution:
        //  |       has.add("javascript", function(){ return true; }, true);
        //
        // example:
        //      Again with the redundantness. You can do this in your tests, but we should
        //      not be doing this in any internal has.js tests
        //  |       has.add("javascript", true);
        //
        // example:
        //      Three things are passed to the testFunction. `global`, `document`, and a generic element
        //      from which to work your test should the need arise.
        //  |       has.add("bug-byid", function(g, d, el){
        //  |           // g  == global, typically window, yadda yadda
        //  |           // d  == document object
        //  |           // el == the generic element. a `has` element.
        //  |           return false; // fake test, byid-when-form-has-name-matching-an-id is slightly longer
        //  |       });
        testCache[name] = now ? test(g, d, el) : test;
    }

    // cssprop adapted from http://gist.github.com/598008 (thanks, ^pi)
    function cssprop(name, el){
        var supported = false,
            capitalized = name.charAt(0).toUpperCase() + name.slice(1),
            length = VENDOR_PREFIXES.length,
            style = el.style;

        if(typeof style[name] == "string"){
            supported = true;
        }else{
            while(length--){
                if(typeof style[VENDOR_PREFIXES[length] + capitalized] == "string"){
                    supported = true;
                    break;
                }
            }
        }
        return supported;
    }

    function clearElement(el){
        if(el){
            while(el.lastChild){
                el.removeChild(el.lastChild);
            }
        }
        return el;
    }

    // Host objects can return type values that are different from their actual
    // data type. The objects we are concerned with usually return non-primitive
    // types of object, function, or unknown.
    function isHostType(object, property){
        var type = typeof object[property];
        return type == 'object' ? !!object[property] : !NON_HOST_TYPES[type];
    }

    has.add = add;
    has.clearElement = clearElement;
    has.cssprop = cssprop;
    has.isHostType = isHostType;
    has._tests = testCache;

    has.add("dom", function(g, d, el){
        return d && el && isHostType(g, "location") && isHostType(d, "documentElement") &&
            isHostType(d, "getElementById") && isHostType(d, "getElementsByName") &&
            isHostType(d, "getElementsByTagName") && isHostType(d, "createComment") &&
            isHostType(d, "createElement") && isHostType(d, "createTextNode") &&
            isHostType(el, "appendChild") && isHostType(el, "insertBefore") &&
            isHostType(el, "removeChild") && isHostType(el, "getAttribute") &&
            isHostType(el, "setAttribute") && isHostType(el, "removeAttribute") &&
            isHostType(el, "style") && typeof el.style.cssText == "string";
    });

    // Stop repeat background-image requests and reduce memory consumption in IE6 SP1
    // http://misterpixel.blogspot.com/2006/09/forensic-analysis-of-ie6.html
    // http://blogs.msdn.com/b/cwilso/archive/2006/11/07/ie-re-downloading-background-images.aspx?PageIndex=1
    // http://support.microsoft.com/kb/823727
    try{
        document.execCommand("BackgroundImageCache", false, true);
    }catch(e){}

    return has;

})(this);

//
// FEATURES.JS
//

(function(has, addtest, cssprop, undefined){

    var STR = "string",
        FN = "function"
    ;

    // FIXME: isn't really native
    // miller device gives "[object Console]" in Opera & Webkit. Object in FF, though. ^pi
    addtest("native-console", function(g){
        return ("console" in g);
    });

    addtest("native-xhr", function(g){
        return has.isHostType(g, "XMLHttpRequest");
    });

    addtest("native-cors-xhr", function(g){
        return has("native-xhr") && ("withCredentials" in new XMLHttpRequest);
    });

    addtest("native-xhr-uploadevents", function(g){
        return has("native-xhr") && ("upload" in new XMLHttpRequest);
    });

    addtest("activex", function(g){
        return has.isHostType(g, "ActiveXObject");
    });

    addtest("activex-enabled", function(g){
        var supported = null;
        if(has("activex")){
            try{
                supported = !!new ActiveXObject("htmlfile");
            }catch(e){
                supported = false;
            }
        }
        return supported;
    });

    addtest("native-navigator", function(g){
        return ("navigator" in g);
    });

    /**
     * Geolocation tests for the new Geolocation API specification:
     * This test is a standards compliant-only test; for more complete
     * testing, including a Google Gears fallback, please see:
     *   http://code.google.com/p/geo-location-javascript/
     * or view a fallback solution using google's geo API:
     *   http://gist.github.com/366184
     */
    addtest("native-geolocation", function(g){
        return has("native-navigator") && ("geolocation" in g.navigator);
    });

    addtest("native-crosswindowmessaging", function(g){
        return ("postMessage" in g);
    });

    addtest("native-orientation",function(g){
        return ("ondeviceorientation" in g);
    });

    /**
     * not sure if there is any point in testing for worker support
     * as an adequate fallback is impossible/pointless
     *
     * ^rw
     */
    addtest("native-worker", function(g){
        return ("Worker" in g);
    });

    addtest("native-sharedworker", function(g){
        return ("SharedWorker" in g);
    });

    addtest("native-eventsource", function(g){
        return ("EventSource" in g);
    });

    // non-browser specific
    addtest("eval-global-scope", function(g){
        var fnId = "__eval" + Number(new Date()),
            supported = false;

        // catch indirect eval call errors (i.e. in such clients as Blackberry 9530)
        try{
            g.eval("var " + fnId + "=true");
        }catch(e){}

        supported = (g[fnId] === true);
        if(supported){
            try{
                delete g[fnId];
            }catch(e){
                g[fnId] = undefined;
            }
        }
        return supported;
    });

    // in chrome incognito mode, openDatabase is truthy, but using it
    //   will throw an exception: http://crbug.com/42380
    // we create a dummy database. there is no way to delete it afterwards. sorry.
    addtest("native-sql-db", function(g){
        var dbname = "hasjstestdb",
            supported = ("openDatabase" in g);

        if(supported){
            try{
                supported = !!openDatabase( dbname, "1.0", dbname, 2e4);
            }catch(e){
                supported = false;
            }
        }
        return supported;
    });

    // FIXME: hosttype
    // FIXME: moz and webkit now ship this prefixed. check all possible prefixes. ^pi
    addtest("native-indexeddb", function(g){
        return ("indexedDB" in g);
    });


    addtest("native-localstorage", function(g){
      //  Thanks Modernizr!
      var supported = false;
      try{
        supported = ("localStorage" in g) && ("setItem" in localStorage);
      }catch(e){}
      return supported;
    });

    addtest("native-sessionstorage", function(g){
      //  Thanks Modernizr!
      var supported = false;
      try{
        supported = ("sessionStorage" in g) && ("setItem" in sessionStorage);
      }catch(e){}
      return supported;
    });

    addtest("native-history-state", function(g){
        return ("history" in g) && ("pushState" in history);
    });

    addtest("native-websockets", function(g){
        return ("WebSocket" in g);
    });

})(has, has.add, has.cssprop);

