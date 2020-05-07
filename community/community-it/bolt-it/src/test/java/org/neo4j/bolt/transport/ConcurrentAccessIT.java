/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.bolt.transport;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.neo4j.bolt.AbstractBoltTransportsTest;
import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.bolt.testing.MessageConditions.msgSuccess;

/**
 * Multiple concurrent users should be able to connect simultaneously. We test this with multiple users running
 * load that they roll back, asserting they don't see each others changes.
 */
@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
public class ConcurrentAccessIT extends AbstractBoltTransportsTest
{
    @Inject
    private Neo4jWithSocket server;

    @BeforeEach
    public void setup( TestInfo testInfo ) throws IOException
    {
        server.setConfigure( getSettingsFunction() );
        server.init( testInfo );
    }

    @AfterEach
    public void cleanup()
    {
        server.shutdownDatabase();
    }

    @ParameterizedTest( name = "{displayName} {2}" )
    @MethodSource( "argumentsProvider" )
    public void shouldRunSimpleStatement( Class<? extends TransportConnection> connectionClass, Neo4jPack neo4jPack, String name ) throws Exception
    {
        initParameters( connectionClass, neo4jPack, name );

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
            private final byte[] init = util.defaultAuth();
            private final byte[] createAndRollback = util.defaultRunExplicitCommitTxAndRollBack( "CREATE (n)" );

            private final byte[] matchAll = util.defaultRunAutoCommitTx( "MATCH (n) RETURN n" );

            @Override
            public Void call() throws Exception
            {
                // Connect
                TransportConnection client = newConnection();
                client.connect( server.lookupDefaultConnector() ).send( util.defaultAcceptedVersions() );
                assertThat( client ).satisfies( util.eventuallyReceivesSelectedProtocolVersion() );

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
                assertThat( client ).satisfies( util.eventuallyReceives( msgSuccess() ) );
            }

            private void createAndRollback( TransportConnection client ) throws Exception
            {
                client.send( createAndRollback );
                assertThat( client ).satisfies( util.eventuallyReceives(
                        msgSuccess(), // begin
                        msgSuccess( message -> assertThat( message ).containsKeys( "t_first", "qid" )
                        .containsEntry( "fields", emptyList() ) ), // run
                        msgSuccess( message -> assertThat( message ).containsKeys( "t_last", "db" ) ), // pull_all
                        msgSuccess() // roll_back
                        ) );

                client.send( matchAll );
                assertThat( client ).satisfies( util.eventuallyReceives(
                        msgSuccess( message -> assertThat( message ).containsKey( "t_first" )
                        .containsEntry( "fields", singletonList( "n" ) ) ), // run
                        msgSuccess( message -> assertThat( message ).containsKeys( "t_last", "db" ) ) ) );// pull_all
            }
        };

    }
}
