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

    var DEFAULT_DATA_URL = window.location.protocol + "//" + window.location.host;
    var DEFAULT_MANAGEMENT_URL = "db/manage/";
//    var DEFAULT_MANAGEMENT_URL = ${DB_MANAGEMENT_URI};
    
    var servers = {};
    var currentServerKey = null;
    
    var isLoaded = false;
    
    var triedLocal = false;
    var web = new neo4j.Web();
    
    
    function onHashChange() {
        var newServerKey = $.bbq.getState( "s" );
        api.setCurrentServer(newServerKey);
    }
    
    function triggerLoadedEvent() {
        isLoaded = true;
        wa.trigger("servers.loaded", servers );
        if(currentServerKey) {
            wa.trigger("servers.current.changed", { current : servers[currentServerKey] } );
        }
    }
    
    //
    // CONSTRUCT
    //
    
    $( window ).bind( "hashchange", onHashChange );
    
    // Fetch available neo4j servers
    wa.prop.get("neo4j-servers", function(key, savedServers){
        
        if( triedLocal === false && (savedServers === null || savedServers === undefined)) {

            // There are no servers defined.
            // Check if there is a local server running
            
            triedLocal = true;
            
            web.get( DEFAULT_MANAGEMENT_URL,
                function() {
                    // There is a local server running, start chatting
                    var localServer = new neo4j.GraphDatabase(DEFAULT_DATA_URL, DEFAULT_MANAGEMENT_URL);
                    
                    servers = {};
                    servers[document.domain] = localServer;
                    triggerLoadedEvent();
                    
                    // Save this 'til next time..
                    persistCurrentServers();
                },
                function() {
                    // No local server running :(
                    servers = {};
                    persistCurrentServers();
                }
            );
        } else {
            var isLegacyFormat = false;
            var item;
            for(var key in savedServers) {
                item = savedServers[key];
                if(item.name) {
                    // Legacy support
                    isLegacyFormat = true;
                    
                    if( item.adminUrl === "/admin/server/" ) {
                        item.adminUrl = "/db/manage/";
                    }
                    
                    servers[item.name] = new neo4j.GraphDatabase(item.restUrl, item.adminUrl);
                } else {
                    servers[key] = new neo4j.GraphDatabase(item.url, item.manageUrl);
                }
            }
            triggerLoadedEvent();
        }
    });
    
    function persistCurrentServers() {
        var jsonServers = {};
        for (var name in servers ) {
            jsonServers[name] = {
                url : servers[name].url,
                manageUrl : servers[name].manageUrl
            };
        }
    
        wa.prop.set("neo4j-servers", jsonServers);
    
    };
    
    //
    // PUBLIC API
    //

    var api = {
            
        isLoaded : function() {
            return isLoaded;
        },
        
        getServers : function() { 
            return servers; 
        },
        
        getServer : function(key) {
            if(servers[key]) {
                return servers[key];
            } else {
                return null;
            }
        },
        
        getServerKey : function(server) {
            for(var key in servers) {
                if(servers[key].url === server.url) {
                    return key;
                }
            }
            
            return null;
        },
        
        getCurrentServer : function() {
            return wa.Servers.getServer(currentServerKey);
        },
        
        setCurrentServer : function(keyOrServer) {
            
            if(typeof(keyOrServer) === "object" ) {
                var key = wa.Servers.getServerKey(keyOrServer);
            } else {
                var key = keyOrServer;
            }
            
            if( key !== currentServerKey ) {
                currentServerKey = key;
                if( servers[key] ) {
                    wa.trigger("servers.current.changed", { current : servers[currentServerKey] } );
                }
            }
        },
        
        setServer : function(key, dataUrl, manageUrl) {
            servers[key] = new neo4j.GraphDatabase(dataUrl, manageUrl);
            wa.trigger("servers.changed", { servers : servers } );
            
            persistCurrentServers();
        },
        
        removeServer : function(key) {
        	if( servers[key] ) {
        		delete(servers[key]);
        		wa.trigger("servers.changed", { servers : servers } );
                
                persistCurrentServers();
        	}
        }
    };

    onHashChange();

    return api;
    
})();