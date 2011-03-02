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
  define(['neo4j/webadmin/data/Search', './databrowser/SimpleView', 'neo4j/webadmin/templates/databrowser', 'lib/backbone'], function(Search, SimpleView, template) {
    var DataBrowserView;
    return DataBrowserView = (function() {
      function DataBrowserView() {
        this.search = __bind(this.search, this);;        DataBrowserView.__super__.constructor.apply(this, arguments);
      }
      __extends(DataBrowserView, Backbone.View);
      DataBrowserView.prototype.template = template;
      DataBrowserView.prototype.events = {
        "keyup #data-console": "search"
      };
      DataBrowserView.prototype.initialize = function(options) {
        this.search = new Search(options.state.server);
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
        return this.search.exec($("#data-console", this.el).val());
      };
      return DataBrowserView;
    })();
  });
}).call(this);
