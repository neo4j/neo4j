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
  */  var __bind = function(fn, me){ return function(){ return fn.apply(me, arguments); }; };
  define(['lib/jquery.flot', 'lib/backbone'], function() {
    var LineChart;
    return LineChart = (function() {
      LineChart.prototype.defaultSettings = {
        label: "",
        xaxis: {
          mode: "time",
          timeformat: "%H:%M:%S",
          min: 0
        },
        yaxis: {},
        legend: {
          position: 'nw'
        },
        grid: {
          hoverable: true
        },
        series: {},
        colors: ["#326a75", "#4f848f", "#a0c2c8", "#00191e"],
        tooltipValueFormatter: function(v) {
          return v;
        }
      };
      function LineChart(el, opts) {
        this.render = __bind(this.render, this);;        this.el = $(el);
        this.settings = _.extend(this.defaultSettings, opts);
      }
      LineChart.prototype.render = function(data, opts) {
        return $.plot(this.el, data, _.extend({}, this.settings, opts));
      };
      return LineChart;
    })();
  });
}).call(this);
