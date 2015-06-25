/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.ndp.transport.socket.integration;

import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.function.Factory;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.ndp.messaging.v1.message.Messages;
import org.neo4j.ndp.transport.socket.client.Connection;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.ndp.messaging.v1.message.Messages.pullAll;
import static org.neo4j.ndp.messaging.v1.message.Messages.run;
import static org.neo4j.ndp.messaging.v1.util.MessageMatchers.msgSuccess;
import static org.neo4j.ndp.transport.socket.integration.TransportTestUtil.acceptedVersions;
import static org.neo4j.ndp.transport.socket.integration.TransportTestUtil.chunk;
import static org.neo4j.ndp.transport.socket.integration.TransportTestUtil.eventuallyRecieves;

/**
 * Multiple concurrent users should be able to connect simultaneously. We test this with multiple users running
 * load that they roll back, asserting they don't see each others changes.
 */
@RunWith( Parameterized.class )
public class ConcurrentAccessIT
{
    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket();

    @Parameterized.Parameter(0)
    public Factory<Connection> cf;

    @Parameterized.Parameter(1)
    public HostnamePort address;

    @Parameterized.Parameters
    public static Collection<Object[]> transports()
    {
        return TransportSessionIT.transports();
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
        List<Callable<Void>> workers = new LinkedList<>(  );
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
            private final byte[] initialize = chunk( Messages.initialize( "TestClient" ) );
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
                Connection client = cf.newInstance();
                client.connect( address ).send( acceptedVersions( 1, 0, 0, 0 ) );
                assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );

                initialize(client);

                for ( int i = 0; i < iterationsToRun; i++ )
                {
                    creaeteAndRollback( client );
                }

                return null;
            }

            private void initialize( Connection client ) throws Exception
            {
                client.send( initialize );
                assertThat( client, eventuallyRecieves(
                    msgSuccess()
                ));
            }

            private void creaeteAndRollback(Connection client) throws Exception
            {
                client.send( createAndRollback );
                assertThat( client, eventuallyRecieves(
                        msgSuccess( map( "fields", asList() ) ),
                        msgSuccess(),
                        msgSuccess( map( "fields", asList() ) ),
                        msgSuccess(),
                        msgSuccess( map( "fields", asList() ) ),
                        msgSuccess()) );

                // Verify no visible data
                client.send( matchAll );
                assertThat( client, eventuallyRecieves(
                        msgSuccess( map( "fields", asList( "n" ) ) ),
                        msgSuccess() ) );

            }
        };

    }
}
