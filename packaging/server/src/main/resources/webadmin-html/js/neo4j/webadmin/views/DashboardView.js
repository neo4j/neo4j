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
  define(['neo4j/webadmin/templates/dashboard/base', './dashboard/DashboardInfoView', './dashboard/DashboardChartsView', 'neo4j/webadmin/views/View', 'lib/backbone'], function(template, DashboardInfoView, DashboardChartsView, View) {
    var DashboardView;
    return DashboardView = (function() {
      function DashboardView() {
        this.detach = __bind(this.detach, this);;
        this.remove = __bind(this.remove, this);;
        this.render = __bind(this.render, this);;
        this.initialize = __bind(this.initialize, this);;        DashboardView.__super__.constructor.apply(this, arguments);
      }
      __extends(DashboardView, View);
      DashboardView.prototype.template = template;
      DashboardView.prototype.initialize = function(opts) {
        this.opts = opts;
        return this.appState = opts.state;
      };
      DashboardView.prototype.render = function() {
        $(this.el).html(this.template({
          server: {
            url: "someurl",
            version: "someversion"
          }
        }));
        this.infoView = new DashboardInfoView(this.opts);
        this.chartsView = new DashboardChartsView(this.opts);
        $("#dashboard-info", this.el).append(this.infoView.el);
        $("#dashboard-charts", this.el).append(this.chartsView.el);
        this.infoView.render();
        this.chartsView.render();
        return this;
      };
      DashboardView.prototype.remove = function() {
        this.infoView.remove();
        this.chartsView.remove();
        return DashboardView.__super__.remove.call(this);
      };
      DashboardView.prototype.detach = function() {
        this.chartsView.unbind();
        return DashboardView.__super__.detach.call(this);
      };
      return DashboardView;
    })();
  });
}).call(this);
