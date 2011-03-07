/*
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
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
  */  require(["neo4j/webadmin/DashboardController", "neo4j/webadmin/DataBrowserController", "neo4j/webadmin/ConsoleController", "neo4j/webadmin/ServerInfoController", "neo4j/webadmin/models/ApplicationState", "neo4j/webadmin/views/BaseView", "neo4j/webadmin/ui/FoldoutWatcher", "lib/neo4js", "lib/jquery", "lib/underscore", "lib/backbone"], function(DashboardController, DataBrowserController, ConsoleController, ServerInfoController, ApplicationState, BaseView, FoldoutWatcher) {
    var appState, foldoutWatcher;
    appState = new ApplicationState;
    appState.set({
      server: new neo4j.GraphDatabase(location.protocol + "//" + location.host)
    });
    new BaseView({
      el: $("body"),
      appState: appState
    });
    new DashboardController(appState);
    new DataBrowserController(appState);
    new ConsoleController(appState);
    new ServerInfoController(appState);
    foldoutWatcher = new FoldoutWatcher;
    foldoutWatcher.init();
    return Backbone.history.start();
  });
}).call(this);
