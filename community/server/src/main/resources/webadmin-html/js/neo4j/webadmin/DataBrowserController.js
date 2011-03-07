(function() {
  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; }, __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) {
    for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; }
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor;
    child.__super__ = parent.prototype;
    return child;
  };
  define(['neo4j/webadmin/data/Search', './views/DataBrowserView', './models/DataBrowserState', './models/DataItem', 'lib/backbone'], function(Search, DataBrowserView, DataBrowserState, DataItem) {
    var DEFAULT_QUERY, DataBrowserController;
    DEFAULT_QUERY = "node:0";
    return DataBrowserController = (function() {
      function DataBrowserController() {
        this.getDataBrowserView = __bind(this.getDataBrowserView, this);;
        this.showResult = __bind(this.showResult, this);;
        this.queryChanged = __bind(this.queryChanged, this);;
        this.search = __bind(this.search, this);;
        this.base = __bind(this.base, this);;
        this.initialize = __bind(this.initialize, this);;        DataBrowserController.__super__.constructor.apply(this, arguments);
      }
      __extends(DataBrowserController, Backbone.Controller);
      DataBrowserController.prototype.routes = {
        "/data/": "base",
        "/data/search/:query/": "search"
      };
      DataBrowserController.prototype.initialize = function(appState) {
        this.appState = appState;
        this.server = appState.get("server");
        this.searcher = new Search(this.server);
        this.dataModel = new DataBrowserState({
          server: this.server
        });
        return this.dataModel.bind("change:query", this.queryChanged);
      };
      DataBrowserController.prototype.base = function() {
        return location.hash = "#/data/search/" + DEFAULT_QUERY + "/";
      };
      DataBrowserController.prototype.search = function(query) {
        this.appState.set({
          mainView: this.getDataBrowserView()
        });
        return this.dataModel.setQuery(query);
      };
      DataBrowserController.prototype.queryChanged = function() {
        var url;
        url = "#/data/search/" + (this.dataModel.getEscapedQuery()) + "/";
        if (location.hash !== url) {
          location.hash = url;
        }
        if (this.dataModel.get("queryOutOfSyncWithData")) {
          return this.searcher.exec(this.dataModel.get("query")).then(this.showResult, this.showResult);
        }
      };
      DataBrowserController.prototype.showResult = function(result) {
        return this.dataModel.setData(result);
      };
      DataBrowserController.prototype.getDataBrowserView = function() {
        var _ref;
        return (_ref = this.dataBrowserView) != null ? _ref : this.dataBrowserView = new DataBrowserView({
          state: this.appState,
          dataModel: this.dataModel
        });
      };
      return DataBrowserController;
    })();
  });
}).call(this);
