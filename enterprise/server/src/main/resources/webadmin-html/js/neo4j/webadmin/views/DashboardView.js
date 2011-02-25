(function() {
  var __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) {
    for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; }
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor;
    child.__super__ = parent.prototype;
    return child;
  };
  define(['neo4j/webadmin/templates/dashboard', 'lib/backbone'], function(template) {
    var DashboardView;
    return DashboardView = (function() {
      function DashboardView() {
        DashboardView.__super__.constructor.apply(this, arguments);
      }
      __extends(DashboardView, Backbone.View);
      DashboardView.prototype.template = template;
      DashboardView.prototype.render = function() {
        $(this.el).html(this.template());
        return this;
      };
      return DashboardView;
    })();
  });
}).call(this);
