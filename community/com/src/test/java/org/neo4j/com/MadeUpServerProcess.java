/**
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
package org.neo4j.com;

import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.test.SubProcess;

public class MadeUpServerProcess extends SubProcess<ServerInterface, Long[]> implements ServerInterface
{
    public static final int PORT = 8888;
    
    private volatile transient MadeUpServer server;
    
    @Override
    protected void startup( Long[] creationTimeAndStoreId ) throws Throwable
    {
        MadeUpCommunicationInterface implementation = new MadeUpImplementation(
                new StoreId( creationTimeAndStoreId[0], creationTimeAndStoreId[1] ) );
        server = new MadeUpServer( implementation, 8888 );
    }

    @Override
    public void awaitStarted()
    {
        try
        {
            while ( server == null )
            {
                Thread.sleep( 10 );
            }
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }
    
    @Override
    public void shutdown()
    {
        if ( server != null )
        {
            server.shutdown();
        }
        new Thread()
        {
            public void run()
            {
                try
                {
                    Thread.sleep( 100 );
                }
                catch ( InterruptedException e )
                {
                    Thread.interrupted();
                }
                shutdownProcess();
            }
        }.start();
    }

    protected void shutdownProcess()
    {
        super.shutdown();
    }
}
