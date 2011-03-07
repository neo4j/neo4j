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
  define(['neo4j/webadmin/templates/console/base', 'neo4j/webadmin/templates/console/console', 'lib/backbone'], function(baseTemplate, consoleTemplate) {
    var ConsoleView;
    return ConsoleView = (function() {
      function ConsoleView() {
        this.scrollToBottomOfConsole = __bind(this.scrollToBottomOfConsole, this);;
        this.renderConsole = __bind(this.renderConsole, this);;
        this.render = __bind(this.render, this);;
        this.wrapperClicked = __bind(this.wrapperClicked, this);;
        this.consoleKeyUp = __bind(this.consoleKeyUp, this);;
        this.initialize = __bind(this.initialize, this);;        ConsoleView.__super__.constructor.apply(this, arguments);
      }
      __extends(ConsoleView, Backbone.View);
      ConsoleView.prototype.events = {
        "keyup #console-input": "consoleKeyUp",
        "click #console-base": "wrapperClicked"
      };
      ConsoleView.prototype.initialize = function(opts) {
        this.appState = opts.appState;
        this.consoleState = opts.consoleState;
        return this.consoleState.bind("change", this.renderConsole);
      };
      ConsoleView.prototype.consoleKeyUp = function(ev) {
        if (ev.keyCode === 13) {
          return this.consoleState.eval($("#console-input").val());
        } else if (ev.keyCode === 38) {
          return this.consoleState.prevHistory();
        } else if (ev.keyCode === 40) {
          return this.consoleState.nextHistory();
        }
      };
      ConsoleView.prototype.wrapperClicked = function(ev) {
        return $("#console-input").focus();
      };
      ConsoleView.prototype.render = function() {
        $(this.el).html(baseTemplate());
        this.renderConsole();
        return this;
      };
      ConsoleView.prototype.renderConsole = function() {
        $("#console", this.el).html(consoleTemplate({
          lines: this.consoleState.get("lines"),
          prompt: this.consoleState.get("prompt"),
          showPrompt: this.consoleState.get("showPrompt")
        }));
        this.delegateEvents();
        $("#console-input").focus();
        return this.scrollToBottomOfConsole();
      };
      ConsoleView.prototype.scrollToBottomOfConsole = function() {
        var wrap;
        wrap = $("#console", this.el);
        if (wrap[0]) {
          return wrap[0].scrollTop = wrap[0].scrollHeight;
        }
      };
      return ConsoleView;
    })();
  });
}).call(this);
