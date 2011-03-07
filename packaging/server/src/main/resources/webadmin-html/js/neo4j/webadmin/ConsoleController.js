(function() {
  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; }, __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) {
    for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; }
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor;
    child.__super__ = parent.prototype;
    return child;
  };
  define(['./models/Console', './views/ConsoleView', 'lib/backbone'], function(Console, ConsoleView) {
    var ConsoleController;
    return ConsoleController = (function() {
      function ConsoleController() {
        this.getConsoleView = __bind(this.getConsoleView, this);;
        this.console = __bind(this.console, this);;
        this.initialize = __bind(this.initialize, this);;        ConsoleController.__super__.constructor.apply(this, arguments);
      }
      __extends(ConsoleController, Backbone.Controller);
      ConsoleController.prototype.routes = {
        "/console/": "console"
      };
      ConsoleController.prototype.initialize = function(appState) {
        this.appState = appState;
        return this.consoleState = new Console({
          server: this.appState.get("server")
        });
      };
      ConsoleController.prototype.console = function() {
        return this.appState.set({
          mainView: this.getConsoleView()
        });
      };
      ConsoleController.prototype.getConsoleView = function() {
        var _ref;
        return (_ref = this.consoleView) != null ? _ref : this.consoleView = new ConsoleView({
          appState: this.appState,
          consoleState: this.consoleState
        });
      };
      return ConsoleController;
    })();
  });
}).call(this);
