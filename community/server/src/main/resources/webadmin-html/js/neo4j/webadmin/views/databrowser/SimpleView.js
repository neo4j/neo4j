(function() {
  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; }, __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) {
    for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; }
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor;
    child.__super__ = parent.prototype;
    return child;
  };
  define(['./NodeView', './RelationshipView', './ListView', 'neo4j/webadmin/templates/databrowser/notfound', 'lib/backbone'], function(NodeView, RelationshipView, ListView, notFoundTemplate) {
    var SimpleView;
    return SimpleView = (function() {
      function SimpleView() {
        this.render = __bind(this.render, this);;        SimpleView.__super__.constructor.apply(this, arguments);
      }
      __extends(SimpleView, Backbone.View);
      SimpleView.prototype.initialize = function(options) {
        this.nodeView = new NodeView;
        this.relationshipView = new RelationshipView;
        this.listView = new ListView;
        this.dataModel = options.dataModel;
        return this.dataModel.bind("change", this.render);
      };
      SimpleView.prototype.render = function() {
        var type, view;
        type = this.dataModel.get("type");
        switch (type) {
          case "node":
            view = this.nodeView;
            break;
          case "relationship":
            view = this.relationshipView;
            break;
          case "set":
            view = this.listView;
            break;
          default:
            $(this.el).html(notFoundTemplate());
            return this;
        }
        view.setDataModel(this.dataModel);
        $(this.el).html(view.render().el);
        return view.delegateEvents();
      };
      return SimpleView;
    })();
  });
}).call(this);
