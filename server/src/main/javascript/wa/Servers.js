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
/**
 * Keeps track of connected servers. Automatically loads saved servers from the
 * admin key/value store. If no servers are specified, this module will attempt
 * to connect to the current domain at default ports.
 */
wa.Servers = (function() {
    
    //
    // PRIVATE
    //

    var serverUrl = window.location.protocol + "//" + window.location.host;
    
    var server = new neo4j.GraphDatabase(serverUrl);
  
    function triggerLoadedEvent() {
        isLoaded = true;
        wa.trigger("servers.loaded", servers );
        if(currentServerKey) {
            wa.trigger("servers.current.changed", { current : servers[currentServerKey] } );
        }
    }
    
    //
    // PUBLIC API
    //

    var api = {
        getCurrentServer : function() {
        	return server;  
        }
    };

    return api;
    
})();