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
  define(['neo4j/webadmin/visualization/VisualGraph', 'neo4j/webadmin/data/ItemUrlResolver', 'neo4j/webadmin/views/databrowser/VisualizationSettingsDialog', 'neo4j/webadmin/views/View', 'neo4j/webadmin/security/HtmlEscaper', 'neo4j/webadmin/templates/databrowser/visualization', 'lib/backbone'], function(VisualGraph, ItemUrlResolver, VisualizationSettingsDialog, View, HtmlEscaper, template) {
    var VisualizedView;
    return VisualizedView = (function() {
      function VisualizedView() {
        this.attach = __bind(this.attach, this);;
        this.detach = __bind(this.detach, this);;
        this.remove = __bind(this.remove, this);;
        this.reflowGraphLayout = __bind(this.reflowGraphLayout, this);;
        this.hideSettingsDialog = __bind(this.hideSettingsDialog, this);;
        this.showSettingsDialog = __bind(this.showSettingsDialog, this);;
        this.getViz = __bind(this.getViz, this);;
        this.settingsChanged = __bind(this.settingsChanged, this);;
        this.render = __bind(this.render, this);;        VisualizedView.__super__.constructor.apply(this, arguments);
      }
      __extends(VisualizedView, View);
      VisualizedView.prototype.events = {
        'click #visualization-show-settings': "showSettingsDialog",
        'click #visualization-reflow': "reflowGraphLayout"
      };
      VisualizedView.prototype.initialize = function(options) {
        this.server = options.server;
        this.appState = options.appState;
        this.dataModel = options.dataModel;
        this.settings = this.appState.getVisualizationSettings();
        return this.settings.bind("change", this.settingsChanged);
      };
      VisualizedView.prototype.render = function() {
        if (this.browserHasRequiredFeatures()) {
          if (this.vizEl != null) {
            this.getViz().detach();
          }
          $(this.el).html(template());
          this.vizEl = $("#visualization", this.el);
          this.getViz().attach(this.vizEl);
          switch (this.dataModel.get("type")) {
            case "node":
              this.visualizeFromNode(this.dataModel.getData().getItem());
              break;
            case "relationship":
              this.visualizeFromRelationships([this.dataModel.getData().getItem()]);
              break;
            case "relationshipList":
              this.visualizeFromRelationships(this.dataModel.getData().getRawRelationships());
          }
        } else {
          this.showBrowserNotSupportedMessage();
        }
        return this;
      };
      VisualizedView.prototype.visualizeFromNode = function(node) {
        return this.getViz().setNode(node);
      };
      VisualizedView.prototype.visualizeFromRelationships = function(rels) {
        var MAX, allNodes, i, nodeDownloadChecklist, nodePromises, rel, _ref;
        MAX = 10;
        nodeDownloadChecklist = {};
        nodePromises = [];
        for (i = 0, _ref = rels.length; (0 <= _ref ? i < _ref : i > _ref); (0 <= _ref ? i += 1 : i -= 1)) {
          rel = rels[i];
          if (i >= MAX) {
            alert("Only showing the first ten in the set, to avoid crashing the visualization. We're working on adding filtering here!");
            break;
          }
          if (!(nodeDownloadChecklist[rel.getStartNodeUrl()] != null)) {
            nodeDownloadChecklist[rel.getStartNodeUrl()] = true;
            nodePromises.push(rel.getStartNode());
          }
          if (!(nodeDownloadChecklist[rel.getEndNodeUrl()] != null)) {
            nodeDownloadChecklist[rel.getStartNodeUrl()] = true;
            nodePromises.push(rel.getEndNode());
          }
        }
        allNodes = neo4j.Promise.join.apply(this, nodePromises);
        return allNodes.then(__bind(function(nodes) {
          return this.getViz().setNodes(nodes);
        }, this));
      };
      VisualizedView.prototype.settingsChanged = function() {
        if (this.viz != null) {
          return this.viz.setLabelProperties(this.settings.getLabelProperties());
        }
      };
      VisualizedView.prototype.getViz = function() {
        var height, width, _ref;
        width = $(document).width() - 40;
        height = $(document).height() - 160;
        (_ref = this.viz) != null ? _ref : this.viz = new VisualGraph(this.server, width, height);
        this.settingsChanged();
        return this.viz;
      };
      VisualizedView.prototype.showSettingsDialog = function() {
        var button;
        if (this.settingsDialog != null) {
          return this.hideSettingsDialog();
        } else {
          button = $("#visualization-show-settings");
          button.addClass("selected");
          return this.settingsDialog = new VisualizationSettingsDialog({
            appState: this.appState,
            baseElement: button,
            closeCallback: this.hideSettingsDialog
          });
        }
      };
      VisualizedView.prototype.hideSettingsDialog = function() {
        if (this.settingsDialog != null) {
          this.settingsDialog.remove();
          delete this.settingsDialog;
          return $("#visualization-show-settings").removeClass("selected");
        }
      };
      VisualizedView.prototype.browserHasRequiredFeatures = function() {
        return Object.prototype.__defineGetter__ != null;
      };
      VisualizedView.prototype.showBrowserNotSupportedMessage = function() {
        return $(this.el).html("<div class='pad'>          <h1>I currently do not support visualization in this browser :(</h1>          <p>I can't find the __defineGetter__ API method, which the visualization lib I use, Arbor.js, needs.</p>          <p>If you really want to use visualization (it's pretty awesome), please consider using Google Chrome, Firefox or Safari.</p>          </div>");
      };
      VisualizedView.prototype.reflowGraphLayout = function() {
        if (this.viz !== null) {
          return this.viz.reflow();
        }
      };
      VisualizedView.prototype.remove = function() {
        if (this.browserHasRequiredFeatures()) {
          this.dataModel.unbind("change:data", this.render);
          this.hideSettingsDialog();
          this.getViz().stop();
        }
        return VisualizedView.__super__.remove.call(this);
      };
      VisualizedView.prototype.detach = function() {
        if (this.browserHasRequiredFeatures()) {
          this.dataModel.unbind("change:data", this.render);
          this.hideSettingsDialog();
          this.getViz().stop();
        }
        return VisualizedView.__super__.detach.call(this);
      };
      VisualizedView.prototype.attach = function(parent) {
        VisualizedView.__super__.attach.call(this, parent);
        if (this.browserHasRequiredFeatures() && (this.vizEl != null)) {
          this.getViz().start();
          return this.dataModel.bind("change:data", this.render);
        }
      };
      return VisualizedView;
    })();
  });
}).call(this);
