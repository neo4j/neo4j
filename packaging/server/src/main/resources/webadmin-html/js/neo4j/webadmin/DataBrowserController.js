(function() {
  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; }, __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) {
    for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; }
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor;
    child.__super__ = parent.prototype;
    return child;
  };
  define(['./views/DataBrowserView', 'lib/backbone'], function(DataBrowserView) {
    var DataBrowserController;
    return DataBrowserController = (function() {
      function DataBrowserController() {
        this.getDataBrowserView = __bind(this.getDataBrowserView, this);;
        this.base = __bind(this.base, this);;
        this.initialize = __bind(this.initialize, this);;        DataBrowserController.__super__.constructor.apply(this, arguments);
      }
      __extends(DataBrowserController, Backbone.Controller);
      DataBrowserController.prototype.routes = {
        "/data/": "base"
      };
      DataBrowserController.prototype.initialize = function(appState) {
        return this.appState = appState;
      };
      DataBrowserController.prototype.base = function() {
        return this.appState.set({
          mainView: this.getDataBrowserView()
        });
      };
      DataBrowserController.prototype.getDataBrowserView = function() {
        var _ref;
        return (_ref = this.dataBrowserView) != null ? _ref : this.dataBrowserView = new DataBrowserView(this.appState);
      };
      return DataBrowserController;
    })();
  });
}).call(this);
