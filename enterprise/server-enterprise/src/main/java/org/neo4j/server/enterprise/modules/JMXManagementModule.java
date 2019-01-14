/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.server.enterprise.modules;

import java.lang.management.ManagementFactory;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanServer;
import javax.management.MalformedObjectNameException;
import javax.management.ObjectName;

import org.neo4j.server.NeoServer;
import org.neo4j.server.enterprise.jmx.ServerManagement;
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
    public void stop()
    {
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
