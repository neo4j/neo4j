(function() {
  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; }, __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) {
    for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; }
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor;
    child.__super__ = parent.prototype;
    return child;
  };
  define(['neo4j/webadmin/templates/dashboard', './DashboardInfoView', 'lib/backbone'], function(template, DashboardInfoView) {
    var DashboardView;
    return DashboardView = (function() {
      function DashboardView() {
        this.render = __bind(this.render, this);;
        this.initialize = __bind(this.initialize, this);;        DashboardView.__super__.constructor.apply(this, arguments);
      }
      __extends(DashboardView, Backbone.View);
      DashboardView.prototype.template = template;
      DashboardView.prototype.initialize = function(opts) {
        this.appState = opts.state;
        return this.infoView = new DashboardInfoView(opts);
      };
      DashboardView.prototype.render = function() {
        $(this.el).html(this.template({
          server: {
            url: "someurl",
            version: "someversion"
          }
        }));
        $("#dashboard-info", this.el).append(this.infoView.render().el);
        return this;
      };
      return DashboardView;
    })();
  });
}).call(this);
