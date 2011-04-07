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
  define(['neo4j/webadmin/security/HtmlEscaper', 'lib/backbone'], function(HtmlEscaper) {
    var Console;
    return Console = (function() {
      function Console() {
        this.pushLines = __bind(this.pushLines, this);;
        this.pushHistory = __bind(this.pushHistory, this);;
        this.parseEvalResult = __bind(this.parseEvalResult, this);;
        this.nextHistory = __bind(this.nextHistory, this);;
        this.prevHistory = __bind(this.prevHistory, this);;
        this.eval = __bind(this.eval, this);;
        this.initialize = __bind(this.initialize, this);;        Console.__super__.constructor.apply(this, arguments);
      }
      __extends(Console, Backbone.Model);
      Console.prototype.defaults = {
        lines: [],
        history: [],
        historyIndex: 0,
        showPrompt: false,
        prompt: ""
      };
      Console.prototype.initialize = function(opts) {
        this.server = opts.server;
        this.eval("init()", false, false);
        return this.htmlEscaper = new HtmlEscaper;
      };
      Console.prototype.eval = function(statement, showStatement, includeInHistory) {
        if (showStatement == null) {
          showStatement = true;
        }
        if (includeInHistory == null) {
          includeInHistory = true;
        }
        this.set({
          "showPrompt": false,
          prompt: ""
        }, {
          silent: true
        });
        if (showStatement) {
          this.pushLines([statement], "gremlin> ");
        }
        if (includeInHistory) {
          this.pushHistory(statement);
        }
        return this.server.manage.console.exec(statement, "awesome", this.parseEvalResult);
      };
      Console.prototype.prevHistory = function() {
        var history, historyIndex, historyItem;
        history = this.get("history");
        historyIndex = this.get("historyIndex");
        if (history.length > historyIndex) {
          historyIndex++;
          historyItem = history[history.length - historyIndex];
          return this.set({
            historyIndex: historyIndex,
            prompt: historyItem
          });
        }
      };
      Console.prototype.nextHistory = function() {
        var history, historyIndex, historyItem;
        history = this.get("history");
        historyIndex = this.get("historyIndex");
        if (historyIndex > 1) {
          historyIndex--;
          historyItem = history[history.length - historyIndex];
          return this.set({
            historyIndex: historyIndex,
            prompt: historyItem
          });
        } else if (historyIndex === 1) {
          historyIndex--;
          return this.set({
            historyIndex: historyIndex,
            prompt: ""
          });
        } else {
          return this.set({
            prompt: ""
          });
        }
      };
      Console.prototype.parseEvalResult = function(result) {
        if (_(result).isString() && result.length > 0) {
          if (result.substr(-1) === "\n") {
            result = result.substr(0, result.length - 1);
          }
          result = result.split("\n");
        } else {
          result = [];
        }
        this.set({
          "showPrompt": true
        }, {
          silent: true
        });
        return this.pushLines(result);
      };
      Console.prototype.pushHistory = function(statement) {
        var history;
        if (statement.length > 0) {
          history = this.get("history");
          if (history.length === 0 || history[history.length - 1] !== statement) {
            return this.set({
              history: history.concat([statement]),
              historyIndex: 0
            }, {
              silent: true
            });
          }
        }
      };
      Console.prototype.pushLines = function(lines, prepend) {
        var line;
        if (prepend == null) {
          prepend = "==> ";
        }
        lines = (function() {
          var _i, _len, _results;
          _results = [];
          for (_i = 0, _len = lines.length; _i < _len; _i++) {
            line = lines[_i];
            _results.push(this.htmlEscaper.escape(prepend + line));
          }
          return _results;
        }).call(this);
        return this.set({
          "lines": this.get("lines").concat(lines)
        });
      };
      return Console;
    })();
  });
}).call(this);
