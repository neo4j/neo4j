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
  define(['lib/DateFormat', 'lib/backbone'], function(DateFormat) {
    var ServerStatistics;
    return ServerStatistics = (function() {
      function ServerStatistics() {
        this.toLocalTimestamps = __bind(this.toLocalTimestamps, this);;
        this.addTimestampsToArray = __bind(this.addTimestampsToArray, this);;
        this.getMetrics = __bind(this.getMetrics, this);;
        this.setMonitorData = __bind(this.setMonitorData, this);;
        this.initialize = __bind(this.initialize, this);;        ServerStatistics.__super__.constructor.apply(this, arguments);
      }
      __extends(ServerStatistics, Backbone.Model);
      ServerStatistics.prototype.initialize = function(options) {
        this.timezoneOffset = (new Date()).getTimezoneOffset() * 60 * 1000;
        this.server = options.server;
        this.heartbeat = this.server.heartbeat;
        this.setMonitorData(this.heartbeat.getCachedData());
        return this.heartbeat.addListener(__bind(function(ev) {
          return this.setMonitorData(ev.allData);
        }, this));
      };
      ServerStatistics.prototype.setMonitorData = function(monitorData) {
        var data, key, update, _ref;
        this.timestamps = this.toLocalTimestamps(monitorData.timestamps);
        update = {};
        _ref = monitorData.data;
        for (key in _ref) {
          data = _ref[key];
          update["metric:" + key] = this.addTimestampsToArray(data, this.timestamps);
        }
        this.set(update);
        return this.trigger("change:metrics");
      };
      ServerStatistics.prototype.getMetrics = function(keys, fromTimestamp, granularity) {
        var indexesToInclude, key, startIndex, val, _i, _len, _results;
        if (fromTimestamp == null) {
          fromTimestamp = 0;
        }
        if (granularity == null) {
          granularity = 10000;
        }
        startIndex = this.getClosestPreceedingTimestampIndex(fromTimestamp);
        if (startIndex === -1) {
          startIndex = 0;
        }
        indexesToInclude = this.getTimestampIndexes(startIndex, granularity);
        _results = [];
        for (_i = 0, _len = keys.length; _i < _len; _i++) {
          key = keys[_i];
          val = this.get("metric:" + key);
          _results.push(val && startIndex < val.length ? this.extractValues(val, indexesToInclude) : []);
        }
        return _results;
      };
      ServerStatistics.prototype.getClosestPreceedingTimestampIndex = function(gmtTimestamp) {
        var i, localTimestamp, _ref;
        localTimestamp = gmtTimestamp - this.timezoneOffset;
        for (i = 0, _ref = this.timestamps.length; (0 <= _ref ? i <= _ref : i >= _ref); (0 <= _ref ? i += 1 : i -= 1)) {
          if (this.timestamps[i] >= localTimestamp) {
            if (i > 0) {
              return i - 1;
            } else {
              return i;
            }
          }
        }
        return 0;
      };
      ServerStatistics.prototype.extractValues = function(array, indexesToExtract) {
        var i, _i, _len, _results;
        _results = [];
        for (_i = 0, _len = indexesToExtract.length; _i < _len; _i++) {
          i = indexesToExtract[_i];
          _results.push(array[i]);
        }
        return _results;
      };
      ServerStatistics.prototype.getTimestampIndexes = function(startIndex, granularity) {
        var delta, i, out, prevTimestamp, _ref;
        out = [];
        prevTimestamp = 0;
        for (i = startIndex, _ref = this.timestamps.length; (startIndex <= _ref ? i <= _ref : i >= _ref); (startIndex <= _ref ? i += 1 : i -= 1)) {
          delta = this.timestamps[i] - prevTimestamp;
          if (delta > granularity) {
            prevTimestamp = this.timestamps[i];
            out.push(i);
          }
        }
        return out;
      };
      ServerStatistics.prototype.addTimestampsToArray = function(data, timestamps) {
        var i, _ref, _results;
        _results = [];
        for (i = 0, _ref = data.length - 1; (0 <= _ref ? i <= _ref : i >= _ref); (0 <= _ref ? i += 1 : i -= 1)) {
          _results.push([timestamps[i], data[i]]);
        }
        return _results;
      };
      ServerStatistics.prototype.toLocalTimestamps = function(timestamps) {
        var t, _i, _len, _results;
        _results = [];
        for (_i = 0, _len = timestamps.length; _i < _len; _i++) {
          t = timestamps[_i];
          _results.push(t - this.timezoneOffset);
        }
        return _results;
      };
      return ServerStatistics;
    })();
  });
}).call(this);
