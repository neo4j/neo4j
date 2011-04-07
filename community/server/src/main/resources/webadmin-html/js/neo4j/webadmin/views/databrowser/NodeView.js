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
  define(['neo4j/webadmin/templates/databrowser/node', './PropertyContainerView', 'lib/backbone'], function(template, PropertyContainerView) {
    var NodeView;
    return NodeView = (function() {
      function NodeView() {
        this.initialize = __bind(this.initialize, this);;        NodeView.__super__.constructor.apply(this, arguments);
      }
      __extends(NodeView, PropertyContainerView);
      NodeView.prototype.initialize = function(opts) {
        if (opts == null) {
          opts = {};
        }
        opts.template = template;
        return NodeView.__super__.initialize.call(this, opts);
      };
      return NodeView;
    })();
  });
}).call(this);
