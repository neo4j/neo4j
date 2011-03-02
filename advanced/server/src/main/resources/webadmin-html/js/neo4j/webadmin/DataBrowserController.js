/*
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
(function() {
  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; }, __hasProp = Object.prototype.hasOwnProperty, __extends = function(child, parent) {
    for (var key in parent) { if (__hasProp.call(parent, key)) child[key] = parent[key]; }
    function ctor() { this.constructor = child; }
    ctor.prototype = parent.prototype;
    child.prototype = new ctor;
    child.__super__ = parent.prototype;
    return child;
  };
  define(['./views/DataBrowserView', './models/DataBrowserState', './models/DataItem', 'lib/backbone'], function(DataBrowserView, DataBrowserState, DataItem) {
    var DataBrowserController;
    return DataBrowserController = (function() {
      function DataBrowserController() {
        this.getDataBrowserView = __bind(this.getDataBrowserView, this);;
        this.showNotFound = __bind(this.showNotFound, this);;
        this.showRelationship = __bind(this.showRelationship, this);;
        this.showNode = __bind(this.showNode, this);;
        this.relationship = __bind(this.relationship, this);;
        this.node = __bind(this.node, this);;
        this.base = __bind(this.base, this);;
        this.initialize = __bind(this.initialize, this);;        DataBrowserController.__super__.constructor.apply(this, arguments);
      }
      __extends(DataBrowserController, Backbone.Controller);
      DataBrowserController.prototype.routes = {
        "/data/": "base",
        "/data/node/:id": "node",
        "/data/relationship/:id": "relationship"
      };
      DataBrowserController.prototype.initialize = function(appState) {
        this.appState = appState;
        this.server = appState.get("server");
        return this.dataModel = new DataBrowserState({
          server: this.server
        });
      };
      DataBrowserController.prototype.base = function() {
        return this.appState.set({
          mainView: this.getDataBrowserView()
        });
      };
      DataBrowserController.prototype.node = function(id) {
        this.base();
        return this.server.node(this.nodeUri(id)).then(this.showNode, this.showNotFound);
      };
      DataBrowserController.prototype.relationship = function(id) {
        this.base();
        return this.server.rel(this.relationshipUri(id)).then(this.showRelationship, this.showNotFound);
      };
      DataBrowserController.prototype.showNode = function(node) {
        return this.dataModel.set({
          "data": node,
          type: "node"
        });
      };
      DataBrowserController.prototype.showRelationship = function(relationship) {
        return this.dataModel.set({
          "data": relationship,
          type: "relationship"
        });
      };
      DataBrowserController.prototype.showNotFound = function() {
        return this.dataModel.set({
          "data": null,
          type: "not-found"
        });
      };
      DataBrowserController.prototype.nodeUri = function(id) {
        return this.server.url + "/db/data/node/" + id;
      };
      DataBrowserController.prototype.relationshipUri = function(id) {
        return this.server.url + "/db/data/relationship/" + id;
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
