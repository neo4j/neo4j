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
  define(['neo4j/webadmin/data/Search', 'neo4j/webadmin/data/ItemUrlResolver', 'neo4j/webadmin/security/HtmlEscaper', './databrowser/TabularView', './databrowser/VisualizedView', './databrowser/CreateRelationshipDialog', 'neo4j/webadmin/views/View', 'neo4j/webadmin/templates/databrowser/base', 'lib/backbone'], function(Search, ItemUrlResolver, HtmlEscaper, TabularView, VisualizedView, CreateRelationshipDialog, View, template) {
    var DataBrowserView;
    return DataBrowserView = (function() {
      function DataBrowserView() {
        this.remove = __bind(this.remove, this);;
        this.switchToTabularView = __bind(this.switchToTabularView, this);;
        this.switchToVisualizedView = __bind(this.switchToVisualizedView, this);;
        this.switchView = __bind(this.switchView, this);;
        this.hideCreateRelationshipDialog = __bind(this.hideCreateRelationshipDialog, this);;
        this.createRelationship = __bind(this.createRelationship, this);;
        this.createNode = __bind(this.createNode, this);;
        this.search = __bind(this.search, this);;
        this.queryChanged = __bind(this.queryChanged, this);;
        this.renderDataView = __bind(this.renderDataView, this);;
        this.render = __bind(this.render, this);;        DataBrowserView.__super__.constructor.apply(this, arguments);
      }
      __extends(DataBrowserView, View);
      DataBrowserView.prototype.template = template;
      DataBrowserView.prototype.events = {
        "keyup #data-console": "search",
        "click #data-create-node": "createNode",
        "click #data-create-relationship": "createRelationship",
        "click #data-switch-view": "switchView"
      };
      DataBrowserView.prototype.initialize = function(options) {
        this.dataModel = options.dataModel;
        this.appState = options.state;
        this.server = options.state.getServer();
        this.htmlEscaper = new HtmlEscaper;
        this.urlResolver = new ItemUrlResolver(this.server);
        this.dataModel.bind("change:query", this.queryChanged);
        return this.switchToTabularView();
      };
      DataBrowserView.prototype.render = function() {
        $(this.el).html(this.template({
          query: this.htmlEscaper.escape(this.dataModel.getQuery()),
          viewType: this.viewType,
          dataType: this.dataModel.getDataType()
        }));
        return this.renderDataView();
      };
      DataBrowserView.prototype.renderDataView = function() {
        this.dataView.attach($("#data-area", this.el).empty());
        this.dataView.render();
        return this;
      };
      DataBrowserView.prototype.queryChanged = function() {
        return $("#data-console", this.el).val(this.dataModel.getQuery());
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
      DataBrowserView.prototype.createRelationship = function() {
        var button;
        if (this.createRelationshipDialog != null) {
          return this.hideCreateRelationshipDialog();
        } else {
          button = $("#data-create-relationship");
          button.addClass("selected");
          return this.createRelationshipDialog = new CreateRelationshipDialog({
            baseElement: button,
            dataModel: this.dataModel,
            server: this.server,
            closeCallback: this.hideCreateRelationshipDialog
          });
        }
      };
      DataBrowserView.prototype.hideCreateRelationshipDialog = function() {
        if (this.createRelationshipDialog != null) {
          this.createRelationshipDialog.remove();
          delete this.createRelationshipDialog;
          return $("#data-create-relationship").removeClass("selected");
        }
      };
      DataBrowserView.prototype.switchView = function(ev) {
        if (this.viewType === "visualized") {
          $(ev.target).removeClass("tabular");
          this.switchToTabularView();
        } else {
          $(ev.target).addClass("tabular");
          this.switchToVisualizedView();
        }
        return this.renderDataView();
      };
      DataBrowserView.prototype.switchToVisualizedView = function() {
        var _ref;
        if (this.dataView != null) {
          this.dataView.detach();
        }
        (_ref = this.visualizedView) != null ? _ref : this.visualizedView = new VisualizedView({
          dataModel: this.dataModel,
          appState: this.appState,
          server: this.server
        });
        this.viewType = "visualized";
        return this.dataView = this.visualizedView;
      };
      DataBrowserView.prototype.switchToTabularView = function() {
        var _ref;
        if (this.dataView != null) {
          this.dataView.detach();
        }
        (_ref = this.tabularView) != null ? _ref : this.tabularView = new TabularView({
          dataModel: this.dataModel,
          appState: this.appState,
          server: this.server
        });
        this.viewType = "tabular";
        return this.dataView = this.tabularView;
      };
      DataBrowserView.prototype.unbind = function() {
        return this.dataModel.unbind("change:query", this.queryChanged);
      };
      DataBrowserView.prototype.detach = function() {
        this.unbind();
        this.hideCreateRelationshipDialog();
        if (this.dataView != null) {
          this.dataView.detach();
        }
        return DataBrowserView.__super__.detach.call(this);
      };
      DataBrowserView.prototype.remove = function() {
        this.unbind();
        this.hideCreateRelationshipDialog();
        return this.dataView.remove();
      };
      return DataBrowserView;
    })();
  });
}).call(this);
