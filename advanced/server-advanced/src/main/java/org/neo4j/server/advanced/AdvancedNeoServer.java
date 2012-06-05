/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.server.advanced;

import java.lang.management.ManagementFactory;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.neo4j.server.CommunityNeoServer;
import org.neo4j.server.advanced.jmx.ServerManagement;
import org.neo4j.server.configuration.Configurator;

public class AdvancedNeoServer extends CommunityNeoServer {

    private ServerManagement serverManagement;

	public AdvancedNeoServer( Configurator configurator )
    {
        this.configurator = configurator;
        init();
    }
    
    @Override
	public void init()
    {
    	super.init();
        try {
	    	serverManagement = new ServerManagement( this );
	        MBeanServer beanServer = ManagementFactory.getPlatformMBeanServer();
			beanServer.registerMBean( serverManagement, new ObjectName( "org.neo4j.ServerManagement" , "restartServer", "lifecycle" ));
		} catch (Exception e) {
			throw new RuntimeException("Unable to initialize advanced Neo4j server, see nested exception.", e);
		}
    }
	
}
