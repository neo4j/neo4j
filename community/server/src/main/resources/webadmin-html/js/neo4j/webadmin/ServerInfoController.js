(function() {
  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; }, __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) {
    for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; }
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor;
    child.__super__ = parent.prototype;
    return child;
  };
  define(['./views/ServerInfoView', 'lib/backbone'], function(ServerInfoView) {
    var ServerInfoController;
    return ServerInfoController = (function() {
      function ServerInfoController() {
        this.getServerInfoView = __bind(this.getServerInfoView, this);;
        this.base = __bind(this.base, this);;
        this.initialize = __bind(this.initialize, this);;        ServerInfoController.__super__.constructor.apply(this, arguments);
      }
      __extends(ServerInfoController, Backbone.Controller);
      ServerInfoController.prototype.routes = {
        "/info/": "base"
      };
      ServerInfoController.prototype.initialize = function(appState) {
        return this.appState = appState;
      };
      ServerInfoController.prototype.base = function() {
        return this.appState.set({
          mainView: this.getServerInfoView()
        });
      };
      ServerInfoController.prototype.getServerInfoView = function() {
        var _ref;
        return (_ref = this.serverInfoView) != null ? _ref : this.serverInfoView = new ServerInfoView(this.appState);
      };
      return ServerInfoController;
    })();
  });
}).call(this);
