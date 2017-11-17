/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import org.hamcrest.CoreMatchers;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.bolt.v1.messaging.message.InitMessage;
import org.neo4j.bolt.v1.transport.socket.client.SecureSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SecureWebSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.bolt.v1.transport.socket.client.WebSocketConnection;
import org.neo4j.function.Factory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.neo4j.bolt.v1.messaging.message.PullAllMessage.pullAll;
import static org.neo4j.bolt.v1.messaging.message.RunMessage.run;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.acceptedVersions;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.chunk;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyReceives;

/**
 * Multiple concurrent users should be able to connect simultaneously. We test this with multiple users running
 * load that they roll back, asserting they don't see each others changes.
 */
@RunWith( Parameterized.class )
public class ConcurrentAccessIT
{
    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket( getClass(), settings ->
            settings.put( GraphDatabaseSettings.auth_enabled.name(), "false" ) );

    @Parameterized.Parameter
    public Factory<TransportConnection> cf;

    @Parameterized.Parameters
    public static Collection<Factory<TransportConnection>> transports()
    {
        return asList( SocketConnection::new, WebSocketConnection::new, SecureSocketConnection::new,
                SecureWebSocketConnection::new );
    }

    @Test
    public void shouldRunSimpleStatement() throws Throwable
    {
        // Given
        int numWorkers = 5;
        int numRequests = 1_000;

        List<Callable<Void>> workers = createWorkers( numWorkers, numRequests );
        ExecutorService exec = Executors.newFixedThreadPool( numWorkers );

        try
        {
            // When & then
            for ( Future<Void> f : exec.invokeAll( workers ) )
            {
                f.get( 60, TimeUnit.SECONDS );
            }
        }
        finally
        {
            exec.shutdownNow();
            exec.awaitTermination( 30, TimeUnit.SECONDS );
        }
    }

    private List<Callable<Void>> createWorkers( int numWorkers, int numRequests ) throws Exception
    {
        List<Callable<Void>> workers = new LinkedList<>();
        for ( int i = 0; i < numWorkers; i++ )
        {
            workers.add( newWorker( numRequests ) );
        }
        return workers;
    }

    private Callable<Void> newWorker( final int iterationsToRun ) throws Exception
    {
        return new Callable<Void>()
        {
            private final byte[] init = chunk( InitMessage.init( "TestClient", emptyMap() ) );
            private final byte[] createAndRollback = chunk(
                    run( "BEGIN" ), pullAll(),
                    run( "CREATE (n)" ), pullAll(),
                    run( "ROLLBACK" ), pullAll() );

            private final byte[] matchAll = chunk(
                    run( "MATCH (n) RETURN n" ), pullAll() );

            @Override
            public Void call() throws Exception
            {
                // Connect
                TransportConnection client = cf.newInstance();
                client.connect( server.lookupDefaultConnector() ).send( acceptedVersions( 1, 0, 0, 0 ) );
                assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );

                init( client );

                for ( int i = 0; i < iterationsToRun; i++ )
                {
                    createAndRollback( client );
                }

                return null;
            }

            private void init( TransportConnection client ) throws Exception
            {
                client.send( init );
                assertThat( client, eventuallyReceives(
                        msgSuccess()
                ) );
            }

            private void createAndRollback( TransportConnection client ) throws Exception
            {
                client.send( createAndRollback );
                assertThat( client, eventuallyReceives(
                        msgSuccess( CoreMatchers.<Map<? extends String,?>>allOf( hasEntry( is( "fields" ), equalTo( emptyList() ) ),
                                hasKey( "result_available_after" ) ) ),
                        msgSuccess(),
                        msgSuccess( CoreMatchers.<Map<? extends String,?>>allOf( hasEntry( is( "fields" ), equalTo( emptyList() ) ),
                                hasKey( "result_available_after" ) ) ),
                        msgSuccess(),
                        msgSuccess( CoreMatchers.<Map<? extends String,?>>allOf( hasEntry( is( "fields" ), equalTo( emptyList() ) ),
                                hasKey( "result_available_after" ) ) ),
                        msgSuccess() ) );

                // Verify no visible data
                client.send( matchAll );
                assertThat( client, eventuallyReceives(
                        msgSuccess(CoreMatchers.<Map<? extends String,?>>allOf( hasEntry( is( "fields" ), equalTo( singletonList( "n" ) ) ),
                                hasKey( "result_available_after" ) ) ),
                        msgSuccess() ) );

            }
        };

    }
}
