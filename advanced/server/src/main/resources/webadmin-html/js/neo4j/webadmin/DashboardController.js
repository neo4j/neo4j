(function() {
  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; }, __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) {
    for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; }
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor;
    child.__super__ = parent.prototype;
    return child;
  };
  define(['./views/DashboardView', 'lib/backbone'], function(DashboardView) {
    var ApplicationController;
    return ApplicationController = (function() {
      function ApplicationController() {
        this.getDashboardView = __bind(this.getDashboardView, this);;
        this.dashboard = __bind(this.dashboard, this);;
        this.initialize = __bind(this.initialize, this);;        ApplicationController.__super__.constructor.apply(this, arguments);
      }
      __extends(ApplicationController, Backbone.Controller);
      ApplicationController.prototype.routes = {
        "": "dashboard"
      };
      ApplicationController.prototype.initialize = function(appState) {
        return this.appState = appState;
      };
      ApplicationController.prototype.dashboard = function() {
        return this.appState.set({
          mainView: this.getDashboardView()
        });
      };
      ApplicationController.prototype.getDashboardView = function() {
        var _ref;
        return (_ref = this.dashboardView) != null ? _ref : this.dashboardView = new DashboardView(this.appState);
      };
      return ApplicationController;
    })();
  });
}).call(this);
