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
  define(['neo4j/webadmin/templates/data/node', 'neo4j/webadmin/templates/data/relationship', 'neo4j/webadmin/templates/data/set', 'neo4j/webadmin/templates/data/notfound', 'lib/backbone'], function(nodeTemplate, relationshipTemplate, setTemplate, notFoundTemplate) {
    var SimpleView;
    return SimpleView = (function() {
      function SimpleView() {
        this.itemChanged = __bind(this.itemChanged, this);;
        this.render = __bind(this.render, this);;        SimpleView.__super__.constructor.apply(this, arguments);
      }
      __extends(SimpleView, Backbone.View);
      SimpleView.prototype.initialize = function(options) {
        this.dataModel = options.dataModel;
        return this.dataModel.bind("change:item", this.itemChanged);
      };
      SimpleView.prototype.render = function() {
        var template, type;
        type = this.dataModel.get("item").get("type");
        switch (type) {
          case "node":
            template = nodeTemplate;
            break;
          case "relationship":
            template = relationshipTemplate;
            break;
          case "set":
            template = setTemplate;
            break;
          default:
            template = notFoundTemplate;
        }
        $(this.el).html(template({
          item: this.dataModel.get("item").get("item")
        }));
        return this;
      };
      SimpleView.prototype.itemChanged = function(ev) {
        return this.render();
      };
      return SimpleView;
    })();
  });
}).call(this);
