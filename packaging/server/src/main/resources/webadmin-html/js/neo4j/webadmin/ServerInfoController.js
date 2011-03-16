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
  define(['./models/ServerInfo', './views/ServerInfoView', 'lib/backbone'], function(ServerInfo, ServerInfoView) {
    var ServerInfoController;
    return ServerInfoController = (function() {
      function ServerInfoController() {
        this.getServerInfoView = __bind(this.getServerInfoView, this);;
        this.bean = __bind(this.bean, this);;
        this.base = __bind(this.base, this);;
        this.initialize = __bind(this.initialize, this);;        ServerInfoController.__super__.constructor.apply(this, arguments);
      }
      __extends(ServerInfoController, Backbone.Controller);
      ServerInfoController.prototype.routes = {
        "/info/": "base",
        "/info/:domain/:name/": "bean"
      };
      ServerInfoController.prototype.initialize = function(appState) {
        this.appState = appState;
        this.serverInfo = new ServerInfo({
          server: this.appState.get("server")
        });
        return this.server = this.appState.get("server");
      };
      ServerInfoController.prototype.base = function() {
        this.appState.set({
          mainView: this.getServerInfoView()
        });
        return this.serverInfo.fetch();
      };
      ServerInfoController.prototype.bean = function(domain, name) {
        this.appState.set({
          mainView: this.getServerInfoView()
        });
        return this.serverInfo.setCurrent(domain, name);
      };
      ServerInfoController.prototype.getServerInfoView = function() {
        return new ServerInfoView({
          appState: this.appState,
          serverInfo: this.serverInfo
        });
      };
      return ServerInfoController;
    })();
  });
}).call(this);
