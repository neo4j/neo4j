(function() {
  var __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) {
    for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; }
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor;
    child.__super__ = parent.prototype;
    return child;
  };
  define(['./views/BaseView', 'lib/backbone'], function(BaseView) {
    var ApplicationController;
    return ApplicationController = (function() {
      function ApplicationController() {
        ApplicationController.__super__.constructor.apply(this, arguments);
      }
      __extends(ApplicationController, Backbone.Controller);
      ApplicationController.prototype.routes = {
        "": "dashboard"
      };
      ApplicationController.prototype.dashboard = function() {};
      return ApplicationController;
    })();
  });
}).call(this);
