(function() {
  var __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) {
    for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; }
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor;
    child.__super__ = parent.prototype;
    return child;
  };
  define(['neo4j/webadmin/templates/server_info', 'lib/backbone'], function(template) {
    var ServerInfoView;
    return ServerInfoView = (function() {
      function ServerInfoView() {
        ServerInfoView.__super__.constructor.apply(this, arguments);
      }
      __extends(ServerInfoView, Backbone.View);
      ServerInfoView.prototype.template = template;
      ServerInfoView.prototype.render = function() {
        $(this.el).html(this.template());
        return this;
      };
      return ServerInfoView;
    })();
  });
}).call(this);
