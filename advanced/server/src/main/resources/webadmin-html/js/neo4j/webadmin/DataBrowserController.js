(function() {
  /*
  Copyright (c) 2002-2011 "Neo Technology,"
  Network Engine for Objects in Lund AB [http://neotechnology.com]

  This file is part of Neo4j.

  Neo4j is free software: you can redistribute it and/or modify
  it under the terms of the GNU Affero General Public License as
  published by the Free Software Foundation, either version 3 of the
  License, or (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU Affero General Public License for more details.

  You should have received a copy of the GNU Affero General Public License
  along with this program. If not, see <http://www.gnu.org/licenses/>.
  */  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; }, __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) {
    for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; }
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor;
    child.__super__ = parent.prototype;
    return child;
  };
  define(['neo4j/webadmin/data/QueuedSearch', './views/DataBrowserView', './views/databrowser/VisualizationSettingsView', './models/DataBrowserState', 'lib/backbone'], function(QueuedSearch, DataBrowserView, VisualizationSettingsView, DataBrowserState) {
    var DataBrowserController;
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
        "/data/search/*query": "search"
      };
      DataBrowserController.prototype.initialize = function(appState) {
        this.appState = appState;
        this.server = appState.get("server");
        this.searcher = new QueuedSearch(this.server);
        this.dataModel = new DataBrowserState({
          server: this.server
        });
        return this.dataModel.bind("change:query", this.queryChanged);
      };
      DataBrowserController.prototype.base = function() {
        return this.queryChanged();
      };
      DataBrowserController.prototype.search = function(query) {
        while (query.charAt(query.length - 1) === "/") {
          query = query.substr(0, query.length - 1);
        }
        this.dataModel.setQuery(query);
        return this.appState.set({
          mainView: this.getDataBrowserView()
        });
      };
      DataBrowserController.prototype.queryChanged = function() {
        var query, url;
        query = this.dataModel.get("query");
        if (query === null) {
          return this.search("0");
        }
        url = "#/data/search/" + query + "/";
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
        return (_ref = this.view) != null ? _ref : this.view = new DataBrowserView({
          state: this.appState,
          dataModel: this.dataModel
        });
      };
      return DataBrowserController;
    })();
  });
}).call(this);
