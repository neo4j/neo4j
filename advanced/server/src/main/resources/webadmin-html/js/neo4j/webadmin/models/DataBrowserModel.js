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
    var DataBrowserModel;
    return DataBrowserModel = (function() {
      function DataBrowserModel() {
        this.initialize = __bind(this.initialize, this);;        DataBrowserModel.__super__.constructor.apply(this, arguments);
      }
      __extends(DataBrowserModel, Backbone.Model);
      DataBrowserModel.prototype.initialize = function(options) {
        return this.server = options.server;
      };
      return DataBrowserModel;
    })();
  });
}).call(this);
