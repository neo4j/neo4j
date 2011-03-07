(function() {
  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; }, __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) {
    for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; }
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor;
    child.__super__ = parent.prototype;
    return child;
  };
  define(['neo4j/webadmin/security/HtmlEscaper', 'lib/backbone'], function(HtmlEscaper) {
    var DataBrowserState;
    return DataBrowserState = (function() {
      function DataBrowserState() {
        this.setData = __bind(this.setData, this);;
        this.setQuery = __bind(this.setQuery, this);;
        this.getEscapedQuery = __bind(this.getEscapedQuery, this);;
        this.initialize = __bind(this.initialize, this);;        DataBrowserState.__super__.constructor.apply(this, arguments);
      }
      __extends(DataBrowserState, Backbone.Model);
      DataBrowserState.prototype.defaults = {
        type: null,
        data: null,
        query: "",
        queryOutOfSyncWithData: true
      };
      DataBrowserState.prototype.initialize = function(options) {
        this.server = options.server;
        return this.escaper = new HtmlEscaper;
      };
      DataBrowserState.prototype.getEscapedQuery = function() {
        return this.escaper.escape(this.get("query"));
      };
      DataBrowserState.prototype.setQuery = function(val, isForCurrentData, opts) {
        if (isForCurrentData == null) {
          isForCurrentData = false;
        }
        if (opts == null) {
          opts = {};
        }
        this.set({
          "queryOutOfSyncWithData": !isForCurrentData
        }, opts);
        return this.set({
          "query": val
        }, opts);
      };
      DataBrowserState.prototype.setData = function(result, basedOnCurrentQuery, opts) {
        if (basedOnCurrentQuery == null) {
          basedOnCurrentQuery = true;
        }
        if (opts == null) {
          opts = {};
        }
        this.set({
          "data": result,
          "queryOutOfSyncWithData": basedOnCurrentQuery
        }, {
          silent: true
        });
        if (result instanceof neo4j.models.Node) {
          return this.set({
            type: "node"
          }, opts);
        } else if (result instanceof neo4j.models.Relationship) {
          return this.set({
            type: "relationship"
          }, opts);
        } else if (_(result).isArray()) {
          return this.set({
            type: "list"
          }, opts);
        } else {
          return this.set({
            "data": null,
            type: "not-found"
          }, opts);
        }
      };
      return DataBrowserState;
    })();
  });
}).call(this);
