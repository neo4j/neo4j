(function() {
  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; }, __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) {
    for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; }
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor;
    child.__super__ = parent.prototype;
    return child;
  };
  define(['lib/backbone'], function() {
    var DataBrowserState;
    return DataBrowserState = (function() {
      function DataBrowserState() {
        this.initialize = __bind(this.initialize, this);;        DataBrowserState.__super__.constructor.apply(this, arguments);
      }
      __extends(DataBrowserState, Backbone.Model);
      DataBrowserState.prototype.initialize = function(options) {
        return this.server = options.server;
      };
      return DataBrowserState;
    })();
  });
}).call(this);
