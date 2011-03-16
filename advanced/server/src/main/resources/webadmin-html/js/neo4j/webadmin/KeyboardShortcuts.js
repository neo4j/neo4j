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
  define(['order!lib/jquery', 'order!lib/jquery.hotkeys'], function() {
    var KeyboardShortcuts;
    return KeyboardShortcuts = (function() {
      KeyboardShortcuts.prototype.shortcuts = {
        "s": "search"
      };
      function KeyboardShortcuts(dashboardController, databrowserController, consoleController, serverInfoController) {
        this.search = __bind(this.search, this);;
        this.init = __bind(this.init, this);;        this.databrowserController = databrowserController;
      }
      KeyboardShortcuts.prototype.init = function() {
        var definition, method, _ref, _results;
        _ref = this.shortcuts;
        _results = [];
        for (definition in _ref) {
          method = _ref[definition];
          _results.push($(document).bind("keyup", definition, this[method]));
        }
        return _results;
      };
      KeyboardShortcuts.prototype.search = function(ev) {
        this.databrowserController.base();
        return setTimeout(function() {
          $("#data-console").val("");
          return $("#data-console").focus();
        }, 1);
      };
      return KeyboardShortcuts;
    })();
  });
}).call(this);
