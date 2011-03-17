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
  define(['lib/DateFormat', 'neo4j/webadmin/ui/Tooltip', 'lib/jquery.flot', 'lib/backbone'], function(DateFormat, Tooltip) {
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
        series: {
          points: {
            show: true
          },
          lines: {
            show: true
          }
        },
        grid: {
          hoverable: true
        },
        colors: ["#490A3D", "#BD1550", "#E97F02", "#F8CA00", "#8A9B0F"],
        tooltipYFormatter: function(v) {
          return Math.round(v);
        },
        tooltipXFormatter: function(v) {
          return DateFormat.format(new Date(v));
        }
      };
      function LineChart(el) {
        this.remove = __bind(this.remove, this);;
        this.render = __bind(this.render, this);;
        this.mouseOverPlot = __bind(this.mouseOverPlot, this);;        this.el = $(el);
        this.settings = this.defaultSettings;
        this.tooltip = new Tooltip({
          closeButton: false
        });
        this.el.bind("plothover", this.mouseOverPlot);
      }
      LineChart.prototype.mouseOverPlot = function(event, pos, item) {
        var x, y;
        if (item) {
          if (this.previousHoverPoint !== item.datapoint) {
            this.previousHoverPoint = item.datapoint;
            x = this.settings.tooltipXFormatter(item.datapoint[0]);
            y = this.settings.tooltipYFormatter(item.datapoint[1]);
            return this.tooltip.show("<b>" + item.series.label + "</b><span class='chart-y'>" + y + "</span><span class='chart-x'>" + x + "</span>", [item.pageX, item.pageY]);
          }
        } else {
          return this.tooltip.hide();
        }
      };
      LineChart.prototype.render = function(data, opts) {
        this.settings = _.extend({}, this.defaultSettings, opts);
        return $.plot(this.el, data, this.settings);
      };
      LineChart.prototype.remove = function() {
        this.el.unbind("plothover", this.mouseOverPlot);
        this.tooltip.remove();
        return this.el.remove();
      };
      return LineChart;
    })();
  });
}).call(this);
