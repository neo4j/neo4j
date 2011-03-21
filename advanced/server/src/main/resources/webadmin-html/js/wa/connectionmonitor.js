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
	// Keep track of server connection
	var isShowingConnectionLostDialog = false;
	
	neo4j.events.bind("web.connection.failed", function(ev, args){
		if (! isShowingConnectionLostDialog ){
			if( wa.Servers.getCurrentServer() !== null) {
				isShowingConnectionLostDialog = true;
				wa.ui.Loading.show("Server connection lost","Attempting to re-establish connection..");
				
				wa.Servers.getCurrentServer().heartbeat.waitForPulse(function() {
					wa.ui.Loading.hide();
					isShowingConnectionLostDialog = false;
				});
				
			} else {
				wa.ui.ErrorBox.showError("Unknown connection problem.");
			}
		}
	});
})();