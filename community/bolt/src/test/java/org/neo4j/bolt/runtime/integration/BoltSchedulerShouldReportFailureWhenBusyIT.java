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

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import java.util.function.Consumer;
import java.util.function.Supplier;

import org.neo4j.bolt.v1.transport.integration.Neo4jWithSocket;
import org.neo4j.bolt.v1.transport.integration.TransportTestUtil;
import org.neo4j.bolt.v1.transport.socket.client.SecureSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SecureWebSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.bolt.v1.transport.socket.client.WebSocketConnection;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.concurrent.OtherThreadRule;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.v1.messaging.message.InitMessage.init;
import static org.neo4j.bolt.v1.messaging.message.PullAllMessage.pullAll;
import static org.neo4j.bolt.v1.messaging.message.RunMessage.run;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgFailure;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.transport.integration.Neo4jWithSocket.DEFAULT_CONNECTOR_KEY;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.chunk;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyReceives;

@RunWith( Parameterized.class )
public class BoltSchedulerShouldReportFailureWhenBusyIT
{
    private AssertableLogProvider logProvider;
    private EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private Neo4jWithSocket server = new Neo4jWithSocket( getClass(), getTestGraphDatabaseFactory(), fsRule::get, getSettingsFunction() );

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( fsRule ).around( server );
    @Rule
    public OtherThreadRule<Integer> spawnedUpdate1 = new OtherThreadRule<>();
    @Rule
    public OtherThreadRule<Integer> spawnedUpdate2 = new OtherThreadRule<>();

    @Parameterized.Parameter
    public Supplier<TransportConnection> connectionCreator;

    private HostnamePort address;

    @Parameterized.Parameters
    public static Collection<Supplier<TransportConnection>> transports()
    {
        return asList( () -> new SecureSocketConnection(), () -> new SocketConnection(), () -> new SecureWebSocketConnection(),
                () -> new WebSocketConnection() );
    }

    protected TestGraphDatabaseFactory getTestGraphDatabaseFactory()
    {
        TestGraphDatabaseFactory factory = new TestGraphDatabaseFactory();

        logProvider = new AssertableLogProvider();

        factory.setInternalLogProvider( logProvider );

        return factory;
    }

    protected Consumer<Map<String,String>> getSettingsFunction()
    {
        return settings ->
        {
            settings.put( GraphDatabaseSettings.auth_enabled.name(), "false" );
            settings.put( new BoltConnector( DEFAULT_CONNECTOR_KEY ).enabled.name(), "TRUE" );
            settings.put( new BoltConnector( DEFAULT_CONNECTOR_KEY ).listen_address.name(), "localhost:0" );
            settings.put( new BoltConnector( DEFAULT_CONNECTOR_KEY ).type.name(), BoltConnector.ConnectorType.BOLT.name() );
            settings.put( new BoltConnector( DEFAULT_CONNECTOR_KEY ).thread_pool_core_size.name(), "0" );
            settings.put( new BoltConnector( DEFAULT_CONNECTOR_KEY ).thread_pool_max_size.name(), "2" );
        };
    }

    @Before
    public void setup() throws Exception
    {
        address = server.lookupDefaultConnector();
    }

    @After
    public void after() throws Exception
    {

    }

    @Test
    public void shouldReportFailureWhenAllThreadsInThreadPoolAreBusy() throws Exception
    {
        TransportConnection connection1 = performHandshake( connectionCreator.get() );
        TransportConnection connection2 = performHandshake( connectionCreator.get() );
        TransportConnection connection3 = performHandshake( connectionCreator.get() );
        TransportConnection connection4 = performHandshake( connectionCreator.get() );

        // Generate a Lock
        createNode( connection1, 100 );
        // Start update request
        updateNode( connection1, 100, 101 );

        // Try to update the same node, these two lines will block all available threads
        Future<Integer> result1 = spawnedUpdate1.execute( state -> updateNodeNoThrow( connection2, 100, 101 ) );
        Future<Integer> result2 = spawnedUpdate2.execute( state -> updateNodeNoThrow( connection3, 100, 101 ) );

        spawnedUpdate1.get().awaitStartExecuting();
        spawnedUpdate2.get().awaitStartExecuting();

        connection4.send( chunk( run( "RETURN 1" ), pullAll() ) );
        assertThat( connection4,
                eventuallyReceives( msgFailure( Status.Request.NoThreadsAvailable, "There is no available thread to serve this request at the moment" ),
                        msgFailure( Status.Request.NoThreadsAvailable, "There is no available thread to serve this request at the moment" ) ) );
    }

    private TransportConnection performHandshake( TransportConnection connection ) throws Exception
    {
        connection.connect( address ).send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) ).send(
                TransportTestUtil.chunk( init( "TestClient/1.1", emptyMap() ) ) );

        assertThat( connection, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( connection, eventuallyReceives( msgSuccess() ) );

        return connection;
    }

    private void createNode( TransportConnection connection, int id ) throws Exception
    {
        connection.send( TransportTestUtil.chunk( run( "BEGIN" ), pullAll(), run( "CREATE (n { id: {id} })", ValueUtils.asMapValue( MapUtil.map( "id", id ) ) ),
                pullAll(), run( "COMMIT" ), pullAll() ) );

        assertThat( connection, eventuallyReceives( msgSuccess(), msgSuccess(), msgSuccess(), msgSuccess(), msgSuccess(), msgSuccess() ) );
    }

    private void updateNode( TransportConnection connection, int oldId, int newId ) throws Exception
    {
        connection.send( TransportTestUtil.chunk( run( "BEGIN" ), pullAll(),
                run( "MATCH (n { id: {oldId} }) SET n.id = {newId}", ValueUtils.asMapValue( MapUtil.map( "oldId", oldId, "newId", newId ) ) ), pullAll() ) );

        assertThat( connection, eventuallyReceives( msgSuccess(), msgSuccess(), msgSuccess(), msgSuccess() ) );
    }

    private int updateNodeNoThrow( TransportConnection connection, int oldId, int newId )
    {
        try
        {
            updateNode( connection, oldId, newId );
        }
        catch ( Throwable t )
        {
            return -1;
        }

        return 0;
    }
}
