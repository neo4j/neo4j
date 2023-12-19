/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.server.enterprise.jmx;

import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;
import org.neo4j.server.NeoServer;

public final class ServerManagement implements ServerManagementMBean
{
    private final NeoServer server;

    public ServerManagement( NeoServer server )
    {
        this.server = server;
    }

    @Override
    public synchronized void restartServer()
    {
        final Log log = server.getDatabase().getGraph().getDependencyResolver().resolveDependency( LogService.class )
                .getUserLog( getClass() );

        Thread thread = new Thread( "Restart server thread" )
        {
            @Override
            public void run()
            {
                log.info( "Restarting server" );
                server.stop();
                server.start();
            }
        };
        thread.setDaemon( false );
        thread.start();

        try
        {
            thread.join();
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }
}
