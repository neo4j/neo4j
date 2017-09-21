/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.server.security.enterprise.auth.integration.bolt;

import org.hamcrest.BaseMatcher;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Description;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.bolt.v1.messaging.message.ResetMessage;
import org.neo4j.bolt.v1.runtime.spi.ImmutableRecord;
import org.neo4j.bolt.v1.transport.integration.Neo4jWithSocket;
import org.neo4j.bolt.v1.transport.integration.TransportTestUtil;
import org.neo4j.bolt.v1.transport.socket.client.SecureSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SecureWebSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.bolt.v1.transport.socket.client.WebSocketConnection;
import org.neo4j.cypher.result.QueryResult;
import org.neo4j.function.Factory;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.concurrent.ThreadingRule;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.allOf;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasEntry;
import static org.hamcrest.Matchers.hasKey;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.bolt.v1.messaging.message.InitMessage.init;
import static org.neo4j.bolt.v1.messaging.message.PullAllMessage.pullAll;
import static org.neo4j.bolt.v1.messaging.message.RunMessage.run;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgFailure;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgIgnored;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgRecord;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyReceives;
import static org.neo4j.graphdb.security.AuthorizationViolationException.PERMISSION_DENIED;
import static org.neo4j.helpers.collection.MapUtil.map;

//@RunWith( Parameterized.class )
public class BoltConnectionManagementIT
{
    private HostnamePort address;

    protected TransportConnection admin;
    protected TransportConnection user;

    @Parameterized.Parameter()
    public Factory<TransportConnection> cf;

    @Parameterized.Parameters
    public static Collection<Factory<TransportConnection>> transports()
    {
        return asList( SocketConnection::new, WebSocketConnection::new, SecureSocketConnection::new,
                SecureWebSocketConnection::new );
    }

    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket( getClass(), getTestGraphDatabaseFactory(),
            getSettingsFunction() );
    @Rule
    public final ThreadingRule threading = new ThreadingRule();

    @Before
    public void setup() throws Exception
    {
        this.admin = cf.newInstance();
        this.user = cf.newInstance();
        this.address = server.lookupDefaultConnector();

        authenticate( admin, "neo4j", "neo4j", "123" );
        createNewUser( admin, "Igor", "123" );
    }

    @After
    public void teardown() throws Exception
    {
        if ( admin != null )
        {
            admin.disconnect();
        }
        if ( user != null )
        {
            user.disconnect();
        }
    }

    protected TestGraphDatabaseFactory getTestGraphDatabaseFactory()
    {
        return new TestEnterpriseGraphDatabaseFactory();
    }

    protected Consumer<Map<String, String>> getSettingsFunction()
    {
        return settings -> settings.put( GraphDatabaseSettings.auth_enabled.name(), "true" );
    }

    /*
    All surface tested here is hidden in 3.1, to possibly be completely removed or reworked later
    =============================================================================================
     */

    // --------------- list connections -------------------

    //@Test
    public void shouldListOwnConnection() throws Throwable
    {
        // When
        admin.send( TransportTestUtil.chunk(
                run( "CALL dbms.listConnections() YIELD username, connectionCount" ),
                pullAll() ) );

        // Then
        Map<String, Long> result = collectConnectionResult( admin, 1 );

        assertTrue( result.containsKey( "neo4j" ) );
        assertTrue( result.get( "neo4j" ) == 1L );
    }

    //@Test
    public void shouldListAllConnections() throws Throwable
    {
        // When
        authenticate( user, "Igor", "123", null );
        admin.send( TransportTestUtil.chunk(
                run( "CALL dbms.listConnections() YIELD username, connectionCount" ),
                pullAll() ) );

        // Then
        Map<String, Long> result = collectConnectionResult( admin, 2 );

        assertTrue( result.containsKey( "neo4j" ) );
        assertTrue( result.get( "neo4j" ) == 1L );
        assertTrue( result.containsKey( "Igor" ) );
        assertTrue( result.get( "Igor" ) == 1L );
    }

    //@Test
    public void shouldNotListConnectionsIfNotAdmin() throws Throwable
    {
        // When
        authenticate( user, "Igor", "123", null );
        user.send( TransportTestUtil.chunk(
                run( "CALL dbms.listConnections() YIELD username, connectionCount" ),
                pullAll() ) );

        // Then
        assertThat( user, eventuallyReceives(
                msgFailure( Status.Security.Forbidden, PERMISSION_DENIED ) ) );
    }

    // --------------- terminate connections -------------------

    //@Test
    public void shouldTerminateConnectionForUser() throws Throwable
    {
        // When
        authenticate( user, "Igor", "123", null );
        admin.send( TransportTestUtil.chunk(
                run( "CALL dbms.terminateConnectionsForUser( 'Igor' ) YIELD username, connectionCount" ),
                pullAll() ) );

        // Then
        Map<String, Long> terminationResult = collectConnectionResult( admin, 1 );
        assertTrue( terminationResult.containsKey( "Igor" ) );
        assertTrue( terminationResult.get( "Igor" ) == 1L );

        admin.send( TransportTestUtil.chunk(
                run( "CALL dbms.listConnections() YIELD username, connectionCount" ),
                pullAll() ) );
        Map<String, Long> listResult = collectConnectionResult( admin, 1 );
        assertTrue( listResult.containsKey( "neo4j" ) );
        assertTrue( listResult.get( "neo4j" ) == 1L );

        verifyConnectionHasTerminated( user );
    }

    //@Test
    public void shouldNotFailWhenTerminatingConnectionsForUserWithNoConnections() throws Throwable
    {
        // When
        admin.send( TransportTestUtil.chunk(
                run( "CALL dbms.terminateConnectionsForUser( 'Igor' ) YIELD username, connectionCount" ),
                pullAll() ) );

        // Then
        Map<String, Long> terminationResult = collectConnectionResult( admin, 1 );
        assertTrue( terminationResult.containsKey( "Igor" ) );
        assertTrue( terminationResult.get( "Igor" ) == 0L );
    }

    //@Test
    public void shouldFailWhenTerminatingConnectionsForNonExistentUser() throws Throwable
    {
        // When
        admin.send( TransportTestUtil.chunk(
                run( "CALL dbms.terminateConnectionsForUser( 'NonExistentUser' ) YIELD username, connectionCount" ),
                pullAll() ) );

        // Then
        assertThat( admin, eventuallyReceives( msgFailure( Status.General.InvalidArguments,
                "User 'NonExistentUser' does not exist." ) ) );
    }

    //@Test
    public void shouldFailWhenTerminatingConnectionsByNonAdmin() throws Throwable
    {
        // When
        authenticate( user, "Igor", "123", null );

        // Then
        assertFailTerminateConnectionForUser( user, "neo4j" );
        assertFailTerminateConnectionForUser( user, "NonExistentUser" );
        assertFailTerminateConnectionForUser( user, "" );
    }

    //@Test
    public void shouldTerminateOwnConnectionIfAdmin() throws Throwable
    {
        assertTerminateOwnConnection( admin, "neo4j" );
    }

    //@Test
    public void shouldTerminateOwnConnectionsIfAdmin() throws Throwable
    {
        authenticate( user, "neo4j", "123", null );
        assertTerminateOwnConnections( admin, user, "neo4j" );
    }

    //@Test
    public void shouldTerminateOwnConnectionIfNonAdmin() throws Throwable
    {
        authenticate( user, "Igor", "123", null );
        assertTerminateOwnConnection( user, "Igor" );
    }

    //@Test
    public void shouldTerminateOwnConnectionsIfNonAdmin() throws Throwable
    {
        // Given
        TransportConnection user2 = cf.newInstance();
        authenticate( user, "Igor", "123", null );
        authenticate( user2, "Igor", "123", null );
        assertTerminateOwnConnections( user, user2, "Igor" );
    }

    // ------------------------------------------

    private static void verifyConnectionHasTerminated( TransportConnection conn ) throws Exception
    {
        try
        {
            conn.recv( 1 );
            fail( "Connection should have terminated" );
        }
        catch ( IOException e )
        {
            // This is what we'd expect for a terminated connection
        }
    }

    private static void assertTerminateOwnConnection( TransportConnection conn, String username ) throws Exception
    {
        // Given
        conn.send( TransportTestUtil.chunk(
                run( "CALL dbms.terminateConnectionsForUser( '" + username + "' ) YIELD username, connectionCount" ),
                pullAll() ) );

        // Then
        verifyConnectionHasTerminated( conn );
    }

    private static void assertTerminateOwnConnections( TransportConnection conn1, TransportConnection conn2, String username ) throws
            Exception
    {
        conn1.send( TransportTestUtil.chunk(
                run( "CALL dbms.terminateConnectionsForUser( '" + username + "' ) YIELD username, connectionCount" ),
                pullAll() ) );

        // Then
        verifyConnectionHasTerminated( conn1 );
        verifyConnectionHasTerminated( conn2 );
    }

    private static void assertFailTerminateConnectionForUser( TransportConnection client, String username ) throws Exception
    {
        client.send( TransportTestUtil.chunk(
                run( "CALL dbms.terminateConnectionsForUser( '" + username + "' ) YIELD username, connectionCount" ),
                pullAll() ) );

        // Then
        assertThat( client, eventuallyReceives(
                msgFailure( Status.Security.Forbidden, PERMISSION_DENIED ),
                msgIgnored()
        ) );

        client.send( TransportTestUtil.chunk( ResetMessage.reset() ) );
        assertThat( client, eventuallyReceives( msgSuccess() ) );
    }

    private void authenticate( TransportConnection client, String username, String password, String newPassword )
            throws Exception
    {
        Map<String, Object> authToken =
                map( "principal", username, "credentials", password, "scheme", "basic" );

        if ( newPassword != null )
        {
            authToken.put( "new_credentials", newPassword );
        }

        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1", authToken ) ) );

        assertThat( client, eventuallyReceives( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyReceives( msgSuccess() ) );
    }

    private static void createNewUser( TransportConnection client, String username, String password ) throws Exception
    {
        client.send( TransportTestUtil.chunk(
                run( "CALL dbms.security.createUser( '" + username + "', '" + password + "', false )" ),
                pullAll() ) );
        assertThat( client, eventuallyReceives( msgSuccess(), msgSuccess() ) );
    }

    private static Map<String, Long> collectConnectionResult( TransportConnection client, int n )
    {
        CollectingMatcher collector = new CollectingMatcher();

        // Then
        assertThat( client, eventuallyReceives(
                msgSuccess( CoreMatchers.<Map<? extends String,?>>allOf( hasEntry(is("fields"), equalTo(asList( "username", "connectionCount" ) )),
                        hasKey( "result_available_after" ) ) )
        ) );

        for ( int i = 0; i < n; i++ )
        {
            assertThat( client, eventuallyReceives( msgRecord( collector ) ) );
        }

        assertThat( client, eventuallyReceives( msgSuccess() ) );

        return collector.result();

    }

    static class CollectingMatcher extends BaseMatcher<QueryResult.Record>
    {
        Map<String, Long> resultMap = new HashMap<>();

        @Override
        public void describeTo( Description description )
        {

        }

        @Override
        public boolean matches( Object o )
        {
            if ( o instanceof ImmutableRecord )
            {
                Object[] fields = ((ImmutableRecord) o).fields();
                resultMap.put( fields[0].toString(), (Long) fields[1] );
                return true;
            }
            return false;
        }

        public Map<String, Long> result()
        {
            return resultMap;
        }
    }
}
