/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.bolt.runtime.integration;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
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
import static java.util.concurrent.TimeUnit.SECONDS;
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
public class BoltSchedulerBusyIT extends AbstractBoltTransportsTest
{
    private AssertableLogProvider internalLogProvider = new AssertableLogProvider();
    private AssertableLogProvider userLogProvider = new AssertableLogProvider();
    private EphemeralFileSystemRule fsRule = new EphemeralFileSystemRule();
    private Neo4jWithSocket server = new Neo4jWithSocket( getClass(), getTestGraphDatabaseFactory(), fsRule::get, getSettingsFunction() );
    private TransportConnection connection1;
    private TransportConnection connection2;
    private TransportConnection connection3;
    private TransportConnection connection4;

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
    }

    @After
    public void cleanup() throws Exception
    {
        close( connection1 );
        close( connection2 );
        close( connection3 );
        close( connection4 );
    }

    @Test
    public void shouldReportFailureWhenAllThreadsInThreadPoolAreBusy() throws Throwable
    {
        // it's enough to get the bolt state machine into streaming mode to have
        // the thread sticked to the connection, causing all the available threads
        // to be busy (logically)
        connection1 = enterStreaming();
        connection2 = enterStreaming();

        try
        {
            connection3 = connectAndPerformBoltHandshake( newConnection() );

            connection3.send( util.chunk( init( "TestClient/1.1", emptyMap() ) ) );
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

    @Test
    public void shouldStopConnectionsWhenRelatedJobIsRejectedOnShutdown() throws Throwable
    {
        // Connect and get two connections into idle state
        connection1 = enterStreaming();
        exitStreaming( connection1 );
        connection2 = enterStreaming();
        exitStreaming( connection2 );

        // Connect and get other set of connections to keep threads busy
        connection3 = enterStreaming();
        connection4 = enterStreaming();

        // Clear any log output till now
        internalLogProvider.clear();

        // Shutdown the server
        server.shutdownDatabase();

        // Expect no scheduling error logs
        userLogProvider.assertNoLogCallContaining(
                "since there are no available threads to serve it at the moment. You can retry at a later time" );
        internalLogProvider.assertNoLogCallContaining( "since there are no available threads to serve it at the moment. You can retry at a later time" );
    }

    private TransportConnection enterStreaming() throws Throwable
    {
        TransportConnection connection = null;
        Throwable error = null;

        // retry couple times because worker threads might seem busy
        for ( int i = 1; i <= 7; i++ )
        {
            try
            {
                connection = newConnection();
                enterStreaming( connection, i );
                error = null;
                return connection;
            }
            catch ( Throwable t )
            {
                // failed to enter the streaming state, record the error and retry
                if ( error == null )
                {
                    error = t;
                }
                else
                {
                    error.addSuppressed( t );
                }

                close( connection );
                SECONDS.sleep( i );
            }
        }

        if ( error != null )
        {
            throw error;
        }

        throw new IllegalStateException( "Unable to enter the streaming state" );
    }

    private void enterStreaming( TransportConnection connection, int sleepSeconds ) throws Exception
    {
        connectAndPerformBoltHandshake( connection );

        connection.send( util.chunk( init( "TestClient/1.1", emptyMap() ) ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );

        SECONDS.sleep( sleepSeconds ); // sleep a bit to allow worker thread return back to the pool

        connection.send( util.chunk( run( "UNWIND RANGE (1, 100) AS x RETURN x" ) ) );
        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );
    }

    private TransportConnection connectAndPerformBoltHandshake( TransportConnection connection ) throws Exception
    {
        connection.connect( address ).send( util.acceptedVersions( 1, 0, 0, 0 ) );
        assertThat( connection, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        return connection;
    }

    private void exitStreaming( TransportConnection connection ) throws Exception
    {
        connection.send( util.chunk( discardAll() ) );

        assertThat( connection, util.eventuallyReceives( msgSuccess() ) );
    }

    private void close( TransportConnection connection )
    {
        if ( connection != null )
        {
            try
            {
                connection.disconnect();
            }
            catch ( IOException ignore )
            {
            }
        }
    }
}
