(function() {
  /*
  Copyright (c) 2002-2018 "Neo Technology,"
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
  */  require(["neo4j/webadmin/modules/databrowser/StandaloneVisualizationRouter", "neo4j/webadmin/modules/moreinfo/MoreInfo", "neo4j/webadmin/modules/loading/GlobalLoadingIndicator", "neo4j/webadmin/modules/connectionmonitor/ConnectionMonitor", "neo4j/webadmin/ApplicationState", "ribcage/security/HtmlEscaper", "lib/jquery", "lib/neo4js", "lib/backbone"], function(StandaloneVisualizationRouter, MoreInfo, GlobalLoadingIndicator, ConnectionMonitor, ApplicationState, HtmlEscaper) {
    var appState, htmlEscaper, modules;
    htmlEscaper = new HtmlEscaper();
    window.htmlEscape = htmlEscaper.escape;
    appState = new ApplicationState;
    appState.set({
      server: new neo4j.GraphDatabase(location.protocol + "//" + location.host)
    });
    modules = [new StandaloneVisualizationRouter, new ConnectionMonitor, new GlobalLoadingIndicator, new MoreInfo];
    return jQuery(function() {
      var m, _i, _len;
      for (_i = 0, _len = modules.length; _i < _len; _i++) {
        m = modules[_i];
        m.init(appState);
      }
      if (Backbone.history) {
        return Backbone.history.start();
      }
    });
  });
}).call(this);
