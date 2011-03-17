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
  define(['neo4j/webadmin/ui/LineChart', 'neo4j/webadmin/templates/dashboard/charts', 'lib/backbone'], function(LineChart, template) {
    var DashboardChartsView;
    return DashboardChartsView = (function() {
      function DashboardChartsView() {
        this.remove = __bind(this.remove, this);;
        this.highlightZoomTab = __bind(this.highlightZoomTab, this);;
        this.highlightChartSwitchTab = __bind(this.highlightChartSwitchTab, this);;
        this.switchZoomClicked = __bind(this.switchZoomClicked, this);;
        this.switchChartClicked = __bind(this.switchChartClicked, this);;
        this.redrawChart = __bind(this.redrawChart, this);;
        this.render = __bind(this.render, this);;
        this.initialize = __bind(this.initialize, this);;        DashboardChartsView.__super__.constructor.apply(this, arguments);
      }
      __extends(DashboardChartsView, Backbone.View);
      DashboardChartsView.prototype.template = template;
      DashboardChartsView.prototype.events = {
        'click .switch-dashboard-chart': 'switchChartClicked',
        'click .switch-dashboard-zoom': 'switchZoomClicked'
      };
      DashboardChartsView.prototype.initialize = function(opts) {
        this.statistics = opts.statistics;
        this.dashboardState = opts.dashboardState;
        this.dashboardState.bind("change:chart", this.redrawChart);
        this.dashboardState.bind("change:zoomLevel", this.redrawChart);
        return this.statistics.bind("change:metrics", this.redrawChart);
      };
      DashboardChartsView.prototype.render = function() {
        $(this.el).html(this.template());
        this.chart = new LineChart($("#monitor-chart"));
        this.redrawChart();
        this.highlightChartSwitchTab("primitives");
        this.highlightZoomTab("six_hours");
        return this;
      };
      DashboardChartsView.prototype.redrawChart = function() {
        var chartDef, data, i, metricKeys, metrics, settings, v, xmin, zoomLevel;
        if (this.chart != null) {
          chartDef = this.dashboardState.getChart();
          zoomLevel = this.dashboardState.getZoomLevel();
          metricKeys = (function() {
            var _i, _len, _ref, _results;
            _ref = chartDef.layers;
            _results = [];
            for (_i = 0, _len = _ref.length; _i < _len; _i++) {
              v = _ref[_i];
              _results.push(v.key);
            }
            return _results;
          })();
          metrics = this.statistics.getMetrics(metricKeys);
          data = (function() {
            var _ref, _results;
            _results = [];
            for (i = 0, _ref = metrics.length; (0 <= _ref ? i < _ref : i > _ref); (0 <= _ref ? i += 1 : i -= 1)) {
              _results.push(_.extend({
                data: metrics[i]
              }, chartDef.layers[i]));
            }
            return _results;
          })();
          xmin = 0;
          if (metrics[0].length > 0) {
            xmin = metrics[0][metrics[0].length - 1][0] - zoomLevel.xSpan;
          }
          settings = {
            xaxis: {
              min: xmin,
              mode: "time",
              timeformat: zoomLevel.timeformat
            }
          };
          return this.chart.render(data, _.extend(chartDef.chartSettings || {}, settings));
        }
      };
      DashboardChartsView.prototype.switchChartClicked = function(ev) {
        this.highlightChartSwitchTab($(ev.target).val());
        return this.dashboardState.setChartByKey($(ev.target).val());
      };
      DashboardChartsView.prototype.switchZoomClicked = function(ev) {
        this.highlightZoomTab($(ev.target).val());
        return this.dashboardState.setZoomLevelByKey($(ev.target).val());
      };
      DashboardChartsView.prototype.highlightChartSwitchTab = function(tabKey) {
        $("button.switch-dashboard-chart", this.el).removeClass("current");
        return $("button.switch-dashboard-chart[value='" + tabKey + "']", this.el).addClass("current");
      };
      DashboardChartsView.prototype.highlightZoomTab = function(tabKey) {
        $("button.switch-dashboard-zoom", this.el).removeClass("current");
        return $("button.switch-dashboard-zoom[value='" + tabKey + "']", this.el).addClass("current");
      };
      DashboardChartsView.prototype.remove = function() {
        this.dashboardState.unbind("change:chart", this.redrawChart);
        this.dashboardState.unbind("change:zoomLevel", this.redrawChart);
        this.statistics.unbind("change:metrics", this.redrawChart);
        if (this.chart != null) {
          this.chart.remove();
        }
        return DashboardChartsView.__super__.remove.call(this);
      };
      return DashboardChartsView;
    })();
  });
}).call(this);
