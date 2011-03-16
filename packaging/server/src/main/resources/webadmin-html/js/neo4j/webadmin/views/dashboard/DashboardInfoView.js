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
  define(['neo4j/webadmin/templates/dashboard/info', 'lib/backbone'], function(template) {
    var DashboardInfoView;
    return DashboardInfoView = (function() {
      function DashboardInfoView() {
        this.remove = __bind(this.remove, this);;
        this.render = __bind(this.render, this);;
        this.initialize = __bind(this.initialize, this);;        DashboardInfoView.__super__.constructor.apply(this, arguments);
      }
      __extends(DashboardInfoView, Backbone.View);
      DashboardInfoView.prototype.template = template;
      DashboardInfoView.prototype.initialize = function(opts) {
        this.primitives = opts.primitives;
        this.diskUsage = opts.diskUsage;
        this.cacheUsage = opts.cacheUsage;
        this.primitives.bind("change", this.render);
        this.diskUsage.bind("change", this.render);
        return this.cacheUsage.bind("change", this.render);
      };
      DashboardInfoView.prototype.render = function() {
        $(this.el).html(this.template({
          primitives: this.primitives,
          diskUsage: this.diskUsage,
          cacheUsage: this.cacheUsage
        }));
        return this;
      };
      DashboardInfoView.prototype.remove = function() {
        this.primitives.unbind("change", this.render);
        this.diskUsage.unbind("change", this.render);
        this.cacheUsage.unbind("change", this.render);
        return DashboardInfoView.__super__.remove.call(this);
      };
      return DashboardInfoView;
    })();
  });
}).call(this);
