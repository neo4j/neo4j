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
  define(['neo4j/webadmin/visualization/VisualGraph', 'neo4j/webadmin/data/ItemUrlResolver', 'neo4j/webadmin/ui/LoadingSpinner', 'neo4j/webadmin/views/View', 'neo4j/webadmin/security/HtmlEscaper', 'neo4j/webadmin/templates/databrowser/visualization', 'lib/backbone'], function(VisualGraph, ItemUrlResolver, LoadingSpinner, View, HtmlEscaper, template) {
    var VisualizedView;
    return VisualizedView = (function() {
      function VisualizedView() {
        this.attach = __bind(this.attach, this);;
        this.detach = __bind(this.detach, this);;
        this.remove = __bind(this.remove, this);;
        this.hideLoader = __bind(this.hideLoader, this);;
        this.showLoader = __bind(this.showLoader, this);;
        this.getViz = __bind(this.getViz, this);;
        this.render = __bind(this.render, this);;        VisualizedView.__super__.constructor.apply(this, arguments);
      }
      __extends(VisualizedView, View);
      VisualizedView.prototype.initialize = function(options) {
        this.server = options.server;
        return this.dataModel = options.dataModel;
      };
      VisualizedView.prototype.render = function() {
        var node;
        if (this.vizEl != null) {
          this.getViz().detach();
        }
        $(this.el).html(template());
        this.vizEl = $("#visualization", this.el);
        this.getViz().attach(this.vizEl);
        switch (this.dataModel.get("type")) {
          case "node":
            node = this.dataModel.getData().getItem();
            return this.getViz().setNode(node);
        }
      };
      VisualizedView.prototype.getViz = function() {
        var height, width, _ref;
        width = $(document).width() - 40;
        height = $(document).height() - 120;
        return (_ref = this.viz) != null ? _ref : this.viz = new VisualGraph(this.server, width, height);
      };
      VisualizedView.prototype.showLoader = function() {
        this.hideLoader();
        this.loader = new LoadingSpinner($(".workarea"));
        return this.loader.show();
      };
      VisualizedView.prototype.hideLoader = function() {
        if (this.loader != null) {
          return this.loader.destroy();
        }
      };
      VisualizedView.prototype.remove = function() {
        this.dataModel.unbind("change:data", this.render);
        this.getViz().stop();
        return VisualizedView.__super__.remove.call(this);
      };
      VisualizedView.prototype.detach = function() {
        this.dataModel.unbind("change:data", this.render);
        this.getViz().stop();
        return VisualizedView.__super__.detach.call(this);
      };
      VisualizedView.prototype.attach = function(parent) {
        VisualizedView.__super__.attach.call(this, parent);
        if (this.vizEl != null) {
          this.getViz().start();
        }
        return this.dataModel.bind("change:data", this.render);
      };
      return VisualizedView;
    })();
  });
}).call(this);
