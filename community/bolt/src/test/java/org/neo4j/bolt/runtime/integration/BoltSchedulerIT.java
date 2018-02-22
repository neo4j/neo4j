/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.bolt.runtime.integration;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.neo4j.bolt.AbstractBoltTransportsTest;
import org.neo4j.bolt.v1.messaging.Neo4jPack;
import org.neo4j.bolt.v1.messaging.Neo4jPackV1;
import org.neo4j.bolt.v1.transport.integration.Neo4jWithSocket;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.cypher.result.QueryResult;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.any;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.v1.messaging.message.InitMessage.init;
import static org.neo4j.bolt.v1.messaging.message.PullAllMessage.pullAll;
import static org.neo4j.bolt.v1.messaging.message.ResetMessage.reset;
import static org.neo4j.bolt.v1.messaging.message.RunMessage.run;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgRecord;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.transport.integration.Neo4jWithSocket.DEFAULT_CONNECTOR_KEY;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyReceives;

@RunWith( Parameterized.class )
public class BoltSchedulerIT extends AbstractBoltTransportsTest
{
    private final int numberOfWriters = 20;
    private final int numberOfReaders = 50;
    private final int numberOfIterations = 5;

    private EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private Neo4jWithSocket server = new Neo4jWithSocket( getClass(), getTestGraphDatabaseFactory(), fsRule::get, getSettingsFunction() );
    private Random rndSleep = new Random();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( fsRule ).around( server );

    private HostnamePort address;
    private AtomicInteger idCounter = new AtomicInteger();

    protected TestGraphDatabaseFactory getTestGraphDatabaseFactory()
    {
        return new TestGraphDatabaseFactory();
    }

    protected Consumer<Map<String,String>> getSettingsFunction()
    {
        return settings ->
        {
            settings.put( GraphDatabaseSettings.auth_enabled.name(), "false" );
            settings.put( new BoltConnector( DEFAULT_CONNECTOR_KEY ).enabled.name(), "TRUE" );
            settings.put( new BoltConnector( DEFAULT_CONNECTOR_KEY ).listen_address.name(), "localhost:0" );
            settings.put( new BoltConnector( DEFAULT_CONNECTOR_KEY ).type.name(), BoltConnector.ConnectorType.BOLT.name() );
            settings.put( new BoltConnector( DEFAULT_CONNECTOR_KEY ).thread_pool_core_size.name(), "5" );
            settings.put( new BoltConnector( DEFAULT_CONNECTOR_KEY ).thread_pool_max_size.name(), "100" );
        };
    }

    @Before
    public void setup() throws Exception
    {
        address = server.lookupDefaultConnector();
    }

    @Test
    public void readerAndWritersShouldWork() throws Exception
    {
        List<TransportConnection> writers = new ArrayList<>();
        List<TransportConnection> readers = new ArrayList<>();
        try
        {
            for ( int i = 0; i < numberOfReaders; i++ )
            {
                readers.add( performHandshake( connectionClass.newInstance() ) );
            }

            for ( int i = 0; i < numberOfWriters; i++ )
            {
                writers.add( performHandshake( connectionClass.newInstance() ) );
            }

            List<CompletableFuture<Void>> allRequests = new ArrayList<>();
            for ( int i = 0; i < numberOfWriters; i++ )
            {
                final TransportConnection currentConnection = writers.get( i );
                allRequests.add( CompletableFuture.runAsync( () -> performWriteQueries( currentConnection, numberOfIterations ) ) );
            }

            for ( int i = 0; i < numberOfReaders; i++ )
            {
                final TransportConnection currentConnection = readers.get( i );
                allRequests.add( CompletableFuture.runAsync( () -> performReadQueries( currentConnection, numberOfIterations ) ) );
            }

            CompletableFuture.allOf( allRequests.toArray( new CompletableFuture[0] ) ).get( 5, TimeUnit.MINUTES );
        }
        finally
        {
            for ( int i = 0; i < readers.size(); i++ )
            {
                readers.get( i ).disconnect();
            }

            for ( int i = 0; i < writers.size(); i++ )
            {
                writers.get( i ).disconnect();
            }
        }
    }

    private TransportConnection performHandshake( TransportConnection connection ) throws Exception
    {
        connection.connect( address ).send( util.acceptedVersions( 1, 0, 0, 0 ) ).send(
                util.chunk( init( "TestClient/1.1", emptyMap() ) ) );

        assertThat( connection, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

        return connection;
    }

    private void performWriteQueries( TransportConnection connection, int numberOfIterations )
    {
        try
        {
            for ( int i = 0; i < numberOfIterations; i++ )
            {
                int id = idCounter.incrementAndGet();
                String label = "LABEL";

                connection.send( util.chunk( run( "BEGIN" ) ) );
                assertThat( connection, util.eventuallyReceives( msgSuccess() ) );
                connection.send( util.chunk( pullAll() ) );
                assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

                connection.send( util.chunk( run( String.format( "CREATE (n:%s { id: %d })", label, id ) ) ) );
                assertThat( connection, util.eventuallyReceives( msgSuccess() ) );
                connection.send( util.chunk( pullAll() ) );
                assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

                connection.send( util.chunk( run( "COMMIT" ) ) );
                assertThat( connection, util.eventuallyReceives( msgSuccess() ) );
                connection.send( util.chunk( pullAll() ) );
                assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

                Thread.sleep( rndSleep.nextInt( 1000 ) );

                connection.send( util.chunk( reset() ) );
                assertThat( connection, util.eventuallyReceives( msgSuccess() ) );
            }
        }
        catch ( Exception ex )
        {
            throw new RuntimeException( ex );
        }
    }

    private void performReadQueries( TransportConnection connection, int numberOfIterations )
    {
        try
        {
            for ( int i = 0; i < numberOfIterations; i++ )
            {
                String label = "LABEL";

                connection.send( util.chunk( run( "BEGIN" ) ) );
                assertThat( connection, util.eventuallyReceives( msgSuccess() ) );
                connection.send( util.chunk( pullAll() ) );
                assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

                connection.send( util.chunk( run( String.format( "MATCH (n:%s) RETURN COUNT(n)", label ) ) ) );
                assertThat( connection, util.eventuallyReceives( msgSuccess() ) );
                connection.send( util.chunk( pullAll() ) );
                assertThat( connection, util.eventuallyReceives( msgRecord( any( QueryResult.Record.class )  ), msgSuccess() ) );

                connection.send( util.chunk( run( "COMMIT" ) ) );
                assertThat( connection, util.eventuallyReceives( msgSuccess() ) );
                connection.send( util.chunk( pullAll() ) );
                assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

                Thread.sleep( rndSleep.nextInt( 1000 ) );

                connection.send( util.chunk( reset() ) );
                assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

                Thread.sleep( rndSleep.nextInt( 1000 ) );
            }
        }
        catch ( Exception ex )
        {
            throw new RuntimeException( ex );
        }
    }

}
