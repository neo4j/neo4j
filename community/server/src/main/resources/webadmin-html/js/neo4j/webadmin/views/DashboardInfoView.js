(function() {
  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; }, __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) {
    for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; }
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor;
    child.__super__ = parent.prototype;
    return child;
  };
  define(['neo4j/webadmin/templates/dashboard_info', 'lib/backbone'], function(template) {
    var DashboardInfoView;
    return DashboardInfoView = (function() {
      function DashboardInfoView() {
        this.render = __bind(this.render, this);;
        this.initialize = __bind(this.initialize, this);;        DashboardInfoView.__super__.constructor.apply(this, arguments);
      }
      __extends(DashboardInfoView, Backbone.View);
      DashboardInfoView.prototype.template = template;
      DashboardInfoView.prototype.initialize = function(opts) {
        console.log(opts);
        this.primitives = opts.primitives;
        this.diskUsage = opts.diskUsage;
        this.cacheUsage = opts.cacheUsage;
        this.primitives.bind("change", this.render);
        this.diskUsage.bind("change", this.render);
        return this.cacheUsage.bind("change", this.render);
      };
      DashboardInfoView.prototype.render = function() {
        $(this.el).html(this.template({
          primitives: this.primitives,
          diskUsage: this.diskUsage,
          cacheUsage: this.cacheUsage
        }));
        return this;
      };
      return DashboardInfoView;
    })();
  });
}).call(this);
