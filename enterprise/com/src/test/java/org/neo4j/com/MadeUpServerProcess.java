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
package org.neo4j.com;

import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.test.subprocess.SubProcess;

public class MadeUpServerProcess extends SubProcess<ServerInterface, StartupData> implements ServerInterface
{
    private static final long serialVersionUID = 1L;

    public static final int PORT = 8888;

    private transient volatile MadeUpServer server;

    @Override
    protected void startup( StartupData data ) throws Throwable
    {
        MadeUpCommunicationInterface implementation = new MadeUpServerImplementation(
                new StoreId( data.creationTime, data.storeId, data.creationTime, data.storeId ) );
        MadeUpServer localServer = new MadeUpServer( implementation, 8888, data.internalProtocolVersion,
                data.applicationProtocolVersion, TxChecksumVerifier.ALWAYS_MATCH, data.chunkSize );
        localServer.init();
        localServer.start();
        // The field being non null is an indication of startup, so assign last
        server = localServer;
    }

    @Override
    public void awaitStarted()
    {
        try
        {
            long endTime = System.currentTimeMillis()+20*1000;
            while ( server == null && System.currentTimeMillis() < endTime )
            {
                Thread.sleep( 10 );
            }
            if ( server == null )
            {
                throw new RuntimeException( "Couldn't start server, wait timeout" );
            }
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }

    @Override
    protected void shutdown( boolean normal )
    {
        if ( server != null )
        {
            try
            {
                server.stop();
                server.shutdown();
            }
            catch ( Throwable throwable )
            {
                throw new RuntimeException( throwable );
            }
        }
        new Thread()
        {
            @Override
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
