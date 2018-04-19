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

import java.util.Map;
import java.util.concurrent.RejectedExecutionException;
import java.util.function.Consumer;

import org.neo4j.bolt.AbstractBoltTransportsTest;
import org.neo4j.bolt.runtime.BoltConnection;
import org.neo4j.bolt.v1.transport.integration.Neo4jWithSocket;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.configuration.BoltConnector;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.fs.EphemeralFileSystemRule;

import static java.util.Collections.emptyMap;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.isA;
import static org.hamcrest.CoreMatchers.startsWith;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.v1.messaging.message.DiscardAllMessage.discardAll;
import static org.neo4j.bolt.v1.messaging.message.InitMessage.init;
import static org.neo4j.bolt.v1.messaging.message.RunMessage.run;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgFailure;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.transport.integration.Neo4jWithSocket.DEFAULT_CONNECTOR_KEY;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyReceives;

@RunWith( Parameterized.class )
public class BoltSchedulerShouldReportFailureWhenBusyIT extends AbstractBoltTransportsTest
{
    private AssertableLogProvider internalLogProvider = new AssertableLogProvider();
    private AssertableLogProvider userLogProvider = new AssertableLogProvider();
    private EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private Neo4jWithSocket server = new Neo4jWithSocket( getClass(), getTestGraphDatabaseFactory(), fsRule::get, getSettingsFunction() );
    private TransportConnection connection1;
    private TransportConnection connection2;
    private TransportConnection connection3;

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( fsRule ).around( server );

    protected TestGraphDatabaseFactory getTestGraphDatabaseFactory()
    {
        TestGraphDatabaseFactory factory = new TestGraphDatabaseFactory();
        factory.setInternalLogProvider( internalLogProvider );
        factory.setUserLogProvider( userLogProvider );
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
            settings.put( new BoltConnector( DEFAULT_CONNECTOR_KEY ).thread_pool_min_size.name(), "0" );
            settings.put( new BoltConnector( DEFAULT_CONNECTOR_KEY ).thread_pool_max_size.name(), "2" );
        };
    }

    @Before
    public void setup() throws Exception
    {
        address = server.lookupDefaultConnector();

        connection1 = newConnection();
        connection2 = newConnection();
        connection3 = newConnection();
    }

    @After
    public void cleanup() throws Exception
    {
        close( connection1 );
        close( connection2 );
        close( connection3 );
    }

    @Test
    public void shouldReportFailureWhenAllThreadsInThreadPoolAreBusy() throws Exception
    {
        // it's enough to get the bolt state machine into streaming mode to have
        // the thread sticked to the connection, causing all the available threads
        // to be busy (logically)
        enterStreaming( connection1 );
        enterStreaming( connection2 );

        try
        {
            connection3 = newConnection();

            connection3.connect( address )
                    .send( util.acceptedVersions( 1, 0, 0, 0 ) )
                    .send( util.chunk( init( "TestClient/1.1", emptyMap() ) ) );

            assertThat( connection3, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
            assertThat( connection3, util.eventuallyReceives(
                    msgFailure( Status.Request.NoThreadsAvailable, "There are no available threads to serve this request at the moment" ) ) );

            userLogProvider.assertContainsMessageContaining(
                    "since there are no available threads to serve it at the moment. You can retry at a later time" );
            internalLogProvider.assertAtLeastOnce( AssertableLogProvider
                    .inLog( startsWith( BoltConnection.class.getPackage().getName() ) )
                    .error(
                        containsString( "since there are no available threads to serve it at the moment. You can retry at a later time" ),
                        isA( RejectedExecutionException.class ) ) );
        }
        finally
        {
            exitStreaming( connection1 );
            exitStreaming( connection2 );
        }
    }

    private void enterStreaming( TransportConnection connection ) throws Exception
    {
        connection.connect( address )
                .send( util.acceptedVersions( 1, 0, 0, 0 ) )
                .send( util.chunk( init( "TestClient/1.1", emptyMap() ) ) )
                .send( util.chunk( run( "UNWIND RANGE (1, 100) AS x RETURN x" ) ) );

        assertThat( connection, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );
    }

    private void exitStreaming( TransportConnection connection ) throws Exception
    {
        connection.send( util.chunk( discardAll() ) );

        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );
    }

    private void close( TransportConnection connection ) throws Exception
    {
        if ( connection != null )
        {
            connection.disconnect();
        }
    }
}
