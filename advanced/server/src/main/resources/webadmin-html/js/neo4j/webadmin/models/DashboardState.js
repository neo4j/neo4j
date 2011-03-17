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
  define(['lib/backbone'], function(PropertyContainer) {
    var DashboardState;
    return DashboardState = (function() {
      function DashboardState() {
        this.setChart = __bind(this.setChart, this);;
        this.setChartByKey = __bind(this.setChartByKey, this);;
        this.setZoomLevel = __bind(this.setZoomLevel, this);;
        this.setZoomLevelByKey = __bind(this.setZoomLevelByKey, this);;
        this.getZoomLevel = __bind(this.getZoomLevel, this);;
        this.getChart = __bind(this.getChart, this);;
        this.initialize = __bind(this.initialize, this);;        DashboardState.__super__.constructor.apply(this, arguments);
      }
      __extends(DashboardState, Backbone.Model);
      DashboardState.prototype.charts = {
        primitives: {
          layers: [
            {
              label: "Nodes",
              key: 'node_count'
            }, {
              label: "Properties",
              key: 'property_count'
            }, {
              label: "Relationships",
              key: 'relationship_count'
            }
          ]
        },
        memory: {
          layers: [
            {
              label: "Memory usage",
              key: 'memory_usage_percent',
              lines: {
                show: true,
                fill: true,
                fillColor: "#4f848f"
              }
            }
          ],
          chartSettings: {
            yaxis: {
              min: 0,
              max: 100
            },
            tooltipYFormatter: function(v) {
              return Math.floor(v) + "%";
            }
          }
        }
      };
      DashboardState.prototype.zoomLevels = {
        year: {
          xSpan: 1000 * 60 * 60 * 24 * 365,
          timeformat: "%d/%m %y"
        },
        month: {
          xSpan: 1000 * 60 * 60 * 24 * 30,
          timeformat: "%d/%m"
        },
        week: {
          xSpan: 1000 * 60 * 60 * 24 * 7,
          timeformat: "%d/%m"
        },
        day: {
          xSpan: 1000 * 60 * 60 * 24,
          timeformat: "%H:%M"
        },
        six_hours: {
          xSpan: 1000 * 60 * 60 * 6,
          timeformat: "%H:%M"
        },
        thirty_minutes: {
          xSpan: 1000 * 60 * 30,
          timeformat: "%H:%M"
        }
      };
      DashboardState.prototype.initialize = function(options) {
        return this.set({
          chart: this.charts.primitives,
          zoomLevel: this.zoomLevels.six_hours
        });
      };
      DashboardState.prototype.getChart = function() {
        return this.get("chart");
      };
      DashboardState.prototype.getZoomLevel = function() {
        return this.get("zoomLevel");
      };
      DashboardState.prototype.setZoomLevelByKey = function(key) {
        return this.setZoomLevel(this.zoomLevels[key]);
      };
      DashboardState.prototype.setZoomLevel = function(zl) {
        return this.set({
          zoomLevel: zl
        });
      };
      DashboardState.prototype.setChartByKey = function(key) {
        return this.setChart(this.charts[key]);
      };
      DashboardState.prototype.setChart = function(chart) {
        return this.set({
          chart: chart
        });
      };
      return DashboardState;
    })();
  });
}).call(this);
