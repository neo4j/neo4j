(function() {
  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; }, __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) {
    for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; }
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor;
    child.__super__ = parent.prototype;
    return child;
  };
  define(['neo4j/webadmin/templates/databrowser/node', './PropertyContainerView', 'lib/backbone'], function(template, PropertyContainerView) {
    var NodeView;
    return NodeView = (function() {
      function NodeView() {
        this.initialize = __bind(this.initialize, this);;        NodeView.__super__.constructor.apply(this, arguments);
      }
      __extends(NodeView, PropertyContainerView);
      NodeView.prototype.initialize = function(opts) {
        if (opts == null) {
          opts = {};
        }
        opts.template = template;
        return NodeView.__super__.initialize.call(this, opts);
      };
      return NodeView;
    })();
  });
}).call(this);
