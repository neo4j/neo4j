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
  define(['neo4j/webadmin/templates/base', 'lib/backbone'], function(template) {
    var BaseView;
    return BaseView = (function() {
      function BaseView() {
        this.initialize = __bind(this.initialize, this);;        BaseView.__super__.constructor.apply(this, arguments);
      }
      __extends(BaseView, Backbone.View);
      BaseView.prototype.template = template;
      BaseView.prototype.initialize = function(options) {
        this.appState = options.appState;
        return this.appState.bind('change:mainView', __bind(function(event) {
          this.mainView = event.attributes.mainView;
          return this.render();
        }, this));
      };
      BaseView.prototype.render = function() {
        $(this.el).html(this.template({
          mainmenu: [
            {
              label: "Dashboard",
              subtitle: "Get a grip",
              url: "#",
              current: location.hash === ""
            }, {
              label: "Data browser",
              subtitle: "Explore and edit",
              url: "#/data/",
              current: location.hash === "#/data/"
            }, {
              label: "Console",
              subtitle: "Power tool",
              url: "#/console/",
              current: location.hash === "#/console/"
            }, {
              label: "Server info",
              subtitle: "Detailed information",
              url: "#/info/",
              current: location.hash === "#/info/"
            }
          ]
        }));
        if (this.mainView != null) {
          $("#contents").append(this.mainView.el);
          this.mainView.render();
          this.mainView.delegateEvents();
        }
        return this;
      };
      return BaseView;
    })();
  });
}).call(this);
