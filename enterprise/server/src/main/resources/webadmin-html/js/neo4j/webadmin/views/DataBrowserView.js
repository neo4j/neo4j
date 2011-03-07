(function() {
  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; }, __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) {
    for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; }
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor;
    child.__super__ = parent.prototype;
    return child;
  };
  define(['neo4j/webadmin/data/Search', 'neo4j/webadmin/data/ItemUrlResolver', './databrowser/SimpleView', 'neo4j/webadmin/templates/data/base', 'lib/backbone'], function(Search, ItemUrlResolver, SimpleView, template) {
    var DataBrowserView;
    return DataBrowserView = (function() {
      function DataBrowserView() {
        this.createNode = __bind(this.createNode, this);;
        this.search = __bind(this.search, this);;        DataBrowserView.__super__.constructor.apply(this, arguments);
      }
      __extends(DataBrowserView, Backbone.View);
      DataBrowserView.prototype.template = template;
      DataBrowserView.prototype.events = {
        "keyup #data-console": "search",
        "click #data-create-node": "createNode"
      };
      DataBrowserView.prototype.initialize = function(options) {
        this.dataModel = options.dataModel;
        this.server = options.state.getServer();
        this.urlResolver = new ItemUrlResolver(this.server);
        return this.dataView = new SimpleView({
          dataModel: options.dataModel
        });
      };
      DataBrowserView.prototype.render = function() {
        $(this.el).html(this.template());
        $("#data-area", this.el).append(this.dataView.el);
        return this;
      };
      DataBrowserView.prototype.search = function(ev) {
        return this.dataModel.setQuery($("#data-console", this.el).val());
      };
      DataBrowserView.prototype.createNode = function() {
        return this.server.node({}).then(__bind(function(node) {
          var id;
          id = this.urlResolver.extractNodeId(node.getSelf());
          this.dataModel.setData(node, true, {
            silent: true
          });
          return this.dataModel.setQuery(id, true);
        }, this));
      };
      return DataBrowserView;
    })();
  });
}).call(this);
