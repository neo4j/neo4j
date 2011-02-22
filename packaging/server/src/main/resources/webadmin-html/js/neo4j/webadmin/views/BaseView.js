(function() {
  var __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) {
    for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; }
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor;
    child.__super__ = parent.prototype;
    return child;
  };
  define(['neo4j/webadmin/templates/base', 'lib/backbone'], function(template) {
    var BaseView;
    return BaseView = (function() {
      function BaseView() {
        BaseView.__super__.constructor.apply(this, arguments);
      }
      __extends(BaseView, Backbone.View);
      BaseView.prototype.template = template;
      BaseView.prototype.render = function() {
        $(this.el).html(this.template());
        return this;
      };
      return BaseView;
    })();
  });
}).call(this);
