/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.v1.transport.integration;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.bolt.v1.messaging.Neo4jPackV1;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;

import static java.util.Collections.emptyMap;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertThat;
import static org.neo4j.bolt.v1.messaging.message.InitMessage.init;
import static org.neo4j.bolt.v1.messaging.message.PullAllMessage.pullAll;
import static org.neo4j.bolt.v1.messaging.message.RunMessage.run;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;

public class ConnectionTerminationIT
{
    private static final int EXECUTION_TIME_SECONDS = 20;

    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private final TransportTestUtil util = new TransportTestUtil( new Neo4jPackV1() );
    private final ExecutorService executor = Executors.newCachedThreadPool();

    @Rule
    public final Neo4jWithSocket server = new Neo4jWithSocket( getClass(),
            new TestGraphDatabaseFactory().setInternalLogProvider( logProvider ),
            settings -> settings.put( GraphDatabaseSettings.auth_enabled.name(), "false" ) );

    @After
    public void tearDown() throws Exception
    {
        executor.shutdownNow();
        executor.awaitTermination( 1, TimeUnit.MINUTES );
    }

    @Test
    public void shouldNotFailWhenConnectionsAreKilled() throws Exception
    {
        Set<TransportConnection> connections = ConcurrentHashMap.newKeySet();
        AtomicBoolean stop = new AtomicBoolean();

        for ( int i = 0; i < 10; i++ )
        {
            executor.submit( () -> runQueriesUntilStopped( connections, stop ) );
        }

        executor.submit( () -> killConnectionsUntilStopped( connections, stop ) );

        SECONDS.sleep( EXECUTION_TIME_SECONDS );
        stop.set( true );

        logProvider.assertNoLogCallContaining( Status.Classification.DatabaseError.toString() );
        logProvider.assertNoLogCallContaining( Status.General.UnknownError.code().serialize() );
    }

    private void runQueriesUntilStopped( Set<TransportConnection> connections, AtomicBoolean stop )
    {
        while ( !stop.get() )
        {
            TransportConnection connection = null;
            try
            {
                connection = new SocketConnection().connect( server.lookupDefaultConnector() );
                connections.add( connection );

                connection.send( util.defaultAcceptedVersions() );
                assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );

                connection.send( util.chunk( init( "TestClient/1.1", emptyMap() ) ) );
                assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

                connection.send( util.chunk(
                        run( "UNWIND range(1, 10000) AS x RETURN x, x * x, 'Hello-' + x" ),
                        pullAll() ) );

                // sleep a bit before disconnecting to allow db start streaming the result
                Thread.sleep( 500 );
            }
            catch ( Throwable ignore )
            {
            }
            finally
            {
                if ( connection != null && connections.remove( connection ) )
                {
                    disconnect( connection );
                }
            }
        }
    }

    private static void killConnectionsUntilStopped( Set<TransportConnection> connections, AtomicBoolean stop )
    {
        while ( !stop.get() )
        {
            Iterator<TransportConnection> iterator = connections.iterator();
            if ( iterator.hasNext() )
            {
                TransportConnection connection = iterator.next();
                if ( connections.remove( connection ) )
                {
                    disconnect( connection );
                }
            }
        }
    }

    private static void disconnect( TransportConnection connection )
    {
        try
        {
            if ( connection != null )
            {
                connection.disconnect();
            }
        }
        catch ( IOException ignore )
        {
        }
    }
}
