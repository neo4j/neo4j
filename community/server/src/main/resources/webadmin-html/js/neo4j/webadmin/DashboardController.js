(function() {
  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; }, __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) {
    for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; }
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor;
    child.__super__ = parent.prototype;
    return child;
  };
  define(['./views/DashboardView', './models/ServerPrimitives', './models/DiskUsage', './models/CacheUsage', 'lib/backbone'], function(DashboardView, ServerPrimitives, DiskUsage, CacheUsage) {
    var DashboardController;
    return DashboardController = (function() {
      function DashboardController() {
        this.getCacheUsage = __bind(this.getCacheUsage, this);;
        this.getDiskUsage = __bind(this.getDiskUsage, this);;
        this.getServerPrimitives = __bind(this.getServerPrimitives, this);;
        this.getDashboardView = __bind(this.getDashboardView, this);;
        this.dashboard = __bind(this.dashboard, this);;
        this.initialize = __bind(this.initialize, this);;        DashboardController.__super__.constructor.apply(this, arguments);
      }
      __extends(DashboardController, Backbone.Controller);
      DashboardController.prototype.routes = {
        "": "dashboard"
      };
      DashboardController.prototype.initialize = function(appState) {
        return this.appState = appState;
      };
      DashboardController.prototype.dashboard = function() {
        return this.appState.set({
          mainView: this.getDashboardView()
        });
      };
      DashboardController.prototype.getDashboardView = function() {
        var _ref;
        return (_ref = this.dashboardView) != null ? _ref : this.dashboardView = new DashboardView({
          state: this.appState,
          primitives: this.getServerPrimitives(),
          diskUsage: this.getDiskUsage(),
          cacheUsage: this.getCacheUsage()
        });
      };
      DashboardController.prototype.getServerPrimitives = function() {
        var _ref;
        return (_ref = this.serverPrimitives) != null ? _ref : this.serverPrimitives = new ServerPrimitives({
          server: this.appState.getServer(),
          pollingInterval: 2000
        });
      };
      DashboardController.prototype.getDiskUsage = function() {
        var _ref;
        return (_ref = this.diskUsage) != null ? _ref : this.diskUsage = new DiskUsage({
          server: this.appState.getServer(),
          pollingInterval: 2000
        });
      };
      DashboardController.prototype.getCacheUsage = function() {
        var _ref;
        return (_ref = this.cacheUsage) != null ? _ref : this.cacheUsage = new CacheUsage({
          server: this.appState.getServer(),
          pollingInterval: 2000
        });
      };
      return DashboardController;
    })();
  });
}).call(this);
