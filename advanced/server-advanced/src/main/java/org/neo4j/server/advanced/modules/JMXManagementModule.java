/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.server.advanced.modules;

import java.lang.management.ManagementFactory;

import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.neo4j.server.NeoServer;
import org.neo4j.server.advanced.jmx.ServerManagement;
import org.neo4j.server.modules.ServerModule;

public class JMXManagementModule implements ServerModule
{
    private final NeoServer server;

    public JMXManagementModule( NeoServer server )
    {
        this.server = server;
    }

    @Override
    public void start()
    {
        try
        {
            ServerManagement serverManagement = new ServerManagement( server );
            MBeanServer beanServer = ManagementFactory.getPlatformMBeanServer();
            beanServer.registerMBean( serverManagement, createObjectName() );
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Unable to initialize jmx management, see nested exception.", e );
        }
    }

    @Override
    public void stop() {
        try
        {
            MBeanServer beanServer = ManagementFactory.getPlatformMBeanServer();
            beanServer.unregisterMBean( createObjectName() );
        }
        catch ( InstanceNotFoundException e )
        {
            // ok
        }
        catch ( Exception e )
        {
            throw new RuntimeException( "Unable to shut down jmx management, see nested exception.", e );
        }
    }

    private ObjectName createObjectName() throws MalformedObjectNameException
    {
        return new ObjectName( "org.neo4j.ServerManagement", "restartServer", "lifecycle" );
    }
}
