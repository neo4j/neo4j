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
package org.neo4j.server.advanced.jmx;

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
        final Log log = server.getDatabase().getGraph().getDependencyResolver().resolveDependency( LogService.class ).getUserLog( getClass() );

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
