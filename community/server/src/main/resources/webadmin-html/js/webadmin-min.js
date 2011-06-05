/*
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
(function() {
  /*
  Copyright (c) 2002-2011 "Neo Technology,"
  Network Engine for Objects in Lund AB [http://neotechnology.com]

  This file is part of Neo4j.

  Neo4j is free software: you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation, either version 3 of the License, or
  (at your option) any later version.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program.  If not, see <http://www.gnu.org/licenses/>.
  */  window.log = function() {
    var item, _i, _len, _results;
    if ($("#messages").length === 0) {
      $("body").append("<ul id='messages'></ul>");
    }
    _results = [];
    for (_i = 0, _len = arguments.length; _i < _len; _i++) {
      item = arguments[_i];
      _results.push($("#messages").append("<li>" + item));
    }
    return _results;
  };
  require(["neo4j/webadmin/DashboardController", "neo4j/webadmin/DataBrowserController", "neo4j/webadmin/ConsoleController", "neo4j/webadmin/ServerInfoController", "neo4j/webadmin/IndexManagerController", "neo4j/webadmin/models/ApplicationState", "neo4j/webadmin/views/BaseView", "neo4j/webadmin/ui/FoldoutWatcher", "neo4j/webadmin/KeyboardShortcuts", "neo4j/webadmin/SplashScreen", "neo4j/webadmin/GlobalLoadingIndicator", "ribcage/security/HtmlEscaper", "lib/jquery", "lib/neo4js", "lib/backbone"], function(DashboardController, DataBrowserController, ConsoleController, ServerInfoController, IndexManagerController, ApplicationState, BaseView, FoldoutWatcher, KeyboardShortcuts, SplashScreen, GlobalLoadingIndicator, HtmlEscaper) {
    var appState, baseView, consoleController, dashboardController, databrowserController, foldoutWatcher, htmlEscaper, indexManagerController, loadingIndicator, serverInfoController, shortcuts, splashScreen;
    htmlEscaper = new HtmlEscaper();
    window.htmlEscape = htmlEscaper.escape;
    splashScreen = new SplashScreen;
    loadingIndicator = new GlobalLoadingIndicator("#global-loading-indicator");
    appState = new ApplicationState;
    appState.set({
      server: new neo4j.GraphDatabase(location.protocol + "//" + location.host)
    });
    baseView = new BaseView({
      appState: appState
    });
    dashboardController = new DashboardController(appState);
    databrowserController = new DataBrowserController(appState);
    consoleController = new ConsoleController(appState);
    serverInfoController = new ServerInfoController(appState);
    indexManagerController = new IndexManagerController(appState);
    foldoutWatcher = new FoldoutWatcher;
    shortcuts = new KeyboardShortcuts(dashboardController, databrowserController, consoleController, serverInfoController);
    return jQuery(function() {
      $("body").append(baseView.el);
      foldoutWatcher.init();
      Backbone.history.start();
      shortcuts.init();
      return loadingIndicator.init();
    });
  });
}).call(this);
