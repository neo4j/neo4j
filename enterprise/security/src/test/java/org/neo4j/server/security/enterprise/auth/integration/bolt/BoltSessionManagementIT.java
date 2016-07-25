/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import org.hamcrest.Description;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.neo4j.bolt.v1.runtime.spi.ImmutableRecord;
import org.neo4j.bolt.v1.runtime.spi.Record;
import org.neo4j.bolt.v1.transport.integration.Neo4jWithSocket;
import org.neo4j.bolt.v1.transport.integration.TransportTestUtil;
import org.neo4j.bolt.v1.transport.socket.client.Connection;
import org.neo4j.bolt.v1.transport.socket.client.SecureSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SecureWebSocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.WebSocketConnection;
import org.neo4j.function.Factory;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.security.enterprise.auth.AuthProcedures;
import org.neo4j.test.TestEnterpriseGraphDatabaseFactory;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.rule.concurrent.ThreadingRule;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.bolt.v1.messaging.message.Messages.init;
import static org.neo4j.bolt.v1.messaging.message.Messages.pullAll;
import static org.neo4j.bolt.v1.messaging.message.Messages.reset;
import static org.neo4j.bolt.v1.messaging.message.Messages.run;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgFailure;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgIgnored;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgRecord;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.eventuallyRecieves;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.api.exceptions.Status.Session.InvalidSession;

@RunWith( Parameterized.class )
public class BoltSessionManagementIT
{
    @Before
    public void setup() throws Exception
    {
        this.admin = cf.newInstance();
        this.user = cf.newInstance();

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

    @Rule
    public Neo4jWithSocket server = new Neo4jWithSocket( getTestGraphDatabaseFactory(), getSettingsFunction() );

    @Rule
    public final ThreadingRule threading = new ThreadingRule();

    protected TestGraphDatabaseFactory getTestGraphDatabaseFactory()
    {
        return new TestEnterpriseGraphDatabaseFactory();
    }

    protected Consumer<Map<Setting<?>, String>> getSettingsFunction()
    {
        return settings -> {
            settings.put( GraphDatabaseSettings.auth_enabled, "true" );
            settings.put( GraphDatabaseSettings.auth_manager, "enterprise-auth-manager" );
        };
    }

    @Parameterized.Parameter( 0 )
    public Factory<Connection> cf;

    @Parameterized.Parameter( 1 )
    public HostnamePort address;

    protected Connection admin;
    protected Connection user;

    private static String SESSION_TERMINATED_MSG = "The session is no longer available, possibly due to termination.";

    @Parameterized.Parameters
    public static Collection<Object[]> transports()
    {
        return asList(
                new Object[]{
                        (Factory<Connection>) SocketConnection::new,
                        new HostnamePort( "localhost:7687" )
                },
                new Object[]{
                        (Factory<Connection>) WebSocketConnection::new,
                        new HostnamePort( "localhost:7687" )
                },
                new Object[]{
                        (Factory<Connection>) SecureSocketConnection::new,
                        new HostnamePort( "localhost:7687" )
                },
                new Object[]{
                        (Factory<Connection>) SecureWebSocketConnection::new,
                        new HostnamePort( "localhost:7687" )
                } );
    }

    // --------------- list sessions -------------------

    @Test
    public void shouldListOwnSession() throws Throwable
    {
        // When
        admin.send( TransportTestUtil.chunk(
                run( "CALL dbms.listSessions() YIELD username, sessionCount" ),
                pullAll() ) );

        // Then
        Map<String, Long> result = collectSessionResult( admin, 1 );

        assertTrue( result.containsKey( "neo4j" ) );
        assertTrue( result.get( "neo4j" ) == 1L );
    }

    @Test
    public void shouldListAllSessions() throws Throwable
    {
        // When
        authenticate( user, "Igor", "123", null );
        admin.send( TransportTestUtil.chunk(
                run( "CALL dbms.listSessions() YIELD username, sessionCount" ),
                pullAll() ) );

        // Then
        Map<String, Long> result = collectSessionResult( admin, 2 );

        assertTrue( result.containsKey( "neo4j" ) );
        assertTrue( result.get( "neo4j" ) == 1L );
        assertTrue( result.containsKey( "Igor" ) );
        assertTrue( result.get( "Igor" ) == 1L );
    }

    @Test
    public void shouldNotListSessionsIfNotAdmin() throws Throwable
    {
        // When
        authenticate( user, "Igor", "123", null );
        user.send( TransportTestUtil.chunk(
                run( "CALL dbms.listSessions() YIELD username, sessionCount" ),
                pullAll() ) );

        // Then
        assertThat( user, eventuallyRecieves(
                msgFailure( Status.Security.Forbidden, AuthProcedures.PERMISSION_DENIED ) ) );
    }

    // --------------- terminate sessions -------------------

    @Test
    public void shouldTerminateSessionForUser() throws Throwable
    {
        // When
        authenticate( user, "Igor", "123", null );
        admin.send( TransportTestUtil.chunk(
                run( "CALL dbms.terminateSessionsForUser( 'Igor' ) YIELD username, sessionCount" ),
                pullAll() ) );

        // Then
        Map<String, Long> terminationResult = collectSessionResult( admin, 1 );
        assertTrue( terminationResult.containsKey( "Igor" ) );
        assertTrue( terminationResult.get( "Igor" ) == 1L );

        admin.send( TransportTestUtil.chunk(
                run( "CALL dbms.listSessions() YIELD username, sessionCount" ),
                pullAll() ) );
        Map<String, Long> listResult = collectSessionResult( admin, 1 );
        assertTrue( listResult.containsKey( "neo4j" ) );
        assertTrue( listResult.get( "neo4j" ) == 1L );

        user.send( TransportTestUtil.chunk(
                run( "MATCH (n) RETURN n" ),
                pullAll() ) );
        assertThat( user, eventuallyRecieves( msgFailure( InvalidSession, SESSION_TERMINATED_MSG ) ) );
    }

    @Test
    public void shouldNotFailWhenTerminatingSessionsForUserWithNoSessions() throws Throwable
    {
        // When
        admin.send( TransportTestUtil.chunk(
                run( "CALL dbms.terminateSessionsForUser( 'Igor' ) YIELD username, sessionCount" ),
                pullAll() ) );

        // Then
        Map<String, Long> terminationResult = collectSessionResult( admin, 1 );
        assertTrue( terminationResult.containsKey( "Igor" ) );
        assertTrue( terminationResult.get( "Igor" ) == 0L );
    }

    @Test
    public void shouldFailWhenTerminatingSessionsForNonExistentUser() throws Throwable
    {
        // When
        admin.send( TransportTestUtil.chunk(
                run( "CALL dbms.terminateSessionsForUser( 'NonExistentUser' ) YIELD username, sessionCount" ),
                pullAll() ) );

        // Then
        assertThat( admin, eventuallyRecieves( msgFailure( Status.Security.InvalidArguments,
                "User 'NonExistentUser' does not exist." ) ) );
    }

    @Test
    public void shouldFailWhenTerminatingSessionsByNonAdmin() throws Throwable
    {
        // When
        authenticate( user, "Igor", "123", null );

        // Then
        assertFailTerminateSessionForUser( user, "neo4j" );
        assertFailTerminateSessionForUser( user, "NonExistentUser" );
        assertFailTerminateSessionForUser( user, "" );
    }

    @Test
    public void shouldTerminateOwnSessionIfAdmin() throws Throwable
    {
        assertTerminateOwnSession( admin, "neo4j" );
    }

    @Test
    public void shouldTerminateOwnSessionsIfAdmin() throws Throwable
    {
        authenticate( user, "neo4j", "123", null );
        assertTerminateOwnSessions( admin, user, "neo4j" );
    }

    @Test
    public void shouldTerminateOwnSessionIfNonAdmin() throws Throwable
    {
        authenticate( user, "Igor", "123", null );
        assertTerminateOwnSession( user, "Igor" );
    }

    @Test
    public void shouldTerminateOwnSessionsIfNonAdmin() throws Throwable
    {
        // Given
        Connection user2 = cf.newInstance();
        authenticate( user, "Igor", "123", null );
        authenticate( user2, "Igor", "123", null );
        assertTerminateOwnSessions( user, user2, "Igor" );
    }

    // ------------------------------------------

    private static void assertTerminateOwnSession( Connection conn, String username ) throws Exception
    {
        // Given
        conn.send( TransportTestUtil.chunk(
                run( "CALL dbms.terminateSessionsForUser( '" + username + "' ) YIELD username, sessionCount" ),
                pullAll() ) );

        // Then
        assertThat( conn, eventuallyRecieves(
                msgSuccess(),
                msgFailure( InvalidSession, SESSION_TERMINATED_MSG )
        ) );
        conn.send( TransportTestUtil.chunk(
                run( "MATCH (n) RETURN n" ),
                pullAll() ) );
        assertThat( conn, eventuallyRecieves(
                msgFailure( InvalidSession, SESSION_TERMINATED_MSG ),
                msgFailure( InvalidSession, SESSION_TERMINATED_MSG )
        ) );
    }

    private static void assertTerminateOwnSessions( Connection conn1, Connection conn2, String username ) throws
            Exception
    {
        conn1.send( TransportTestUtil.chunk(
                run( "CALL dbms.terminateSessionsForUser( '" + username + "' ) YIELD username, sessionCount" ),
                pullAll() ) );

        // Then
        assertThat( conn1, eventuallyRecieves(
                msgSuccess(),
                msgFailure( InvalidSession, SESSION_TERMINATED_MSG )
        ) );

        conn2.send( TransportTestUtil.chunk(
                run( "MATCH (n) RETURN n" ),
                pullAll() ) );
        assertThat( conn2, eventuallyRecieves(
                msgFailure( InvalidSession, SESSION_TERMINATED_MSG ),
                msgFailure( InvalidSession, SESSION_TERMINATED_MSG )
        ) );
    }

    private static void assertFailTerminateSessionForUser( Connection client, String username ) throws Exception
    {
        client.send( TransportTestUtil.chunk(
                run( "CALL dbms.terminateSessionsForUser( '" + username + "' ) YIELD username, sessionCount" ),
                pullAll() ) );

        // Then
        assertThat( client, eventuallyRecieves(
                msgFailure( Status.Security.Forbidden, AuthProcedures.PERMISSION_DENIED ),
                msgIgnored()
        ) );

        client.send( TransportTestUtil.chunk( reset() ) );
        assertThat( client, eventuallyRecieves( msgSuccess() ) );
    }

    private void authenticate( Connection client, String username, String password, String newPassword )
            throws Exception
    {
        Map<String, Object> authToken =
                map( "principal", username, "credentials", password, "scheme", "basic" );

        if ( newPassword != null)
        {
            authToken.put( "new_credentials", newPassword );
        }

        client.connect( address )
                .send( TransportTestUtil.acceptedVersions( 1, 0, 0, 0 ) )
                .send( TransportTestUtil.chunk(
                        init( "TestClient/1.1", authToken ) ) );

        assertThat( client, eventuallyRecieves( new byte[]{0, 0, 0, 1} ) );
        assertThat( client, eventuallyRecieves( msgSuccess() ) );
    }

    private static void createNewUser( Connection client, String username, String password ) throws Exception
    {
        client.send( TransportTestUtil.chunk(
                run( "CALL dbms.createUser( '" + username + "', '" + password + "', false )" ),
                pullAll() ) );
        assertThat( client, eventuallyRecieves( msgSuccess(), msgSuccess() ) );
    }

    private static Map<String, Long> collectSessionResult( Connection client, int n )
    {
        CollectingMatcher collector = new CollectingMatcher();

        // Then
        assertThat( client, eventuallyRecieves(
                msgSuccess( map( "fields", asList( "username", "sessionCount" ) ) )
            ) );

        for ( int i = 0; i < n; i++ )
        {
            assertThat( client, eventuallyRecieves( msgRecord( collector ) ) );
        }

        assertThat( client, eventuallyRecieves( msgSuccess() ) );

        return collector.result();

    }

    static class CollectingMatcher extends BaseMatcher<Record>
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
