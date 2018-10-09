/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.net;

import org.junit.After;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;

import java.io.IOException;
import java.net.SocketException;
import java.net.URI;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import javax.ws.rs.core.HttpHeaders;

import org.neo4j.bolt.v1.messaging.request.InitMessage;
import org.neo4j.bolt.v1.messaging.request.PullAllMessage;
import org.neo4j.bolt.v1.messaging.request.RunMessage;
import org.neo4j.bolt.v1.transport.integration.TransportTestUtil;
import org.neo4j.bolt.v1.transport.socket.client.SocketConnection;
import org.neo4j.bolt.v1.transport.socket.client.TransportConnection;
import org.neo4j.bolt.v2.messaging.Neo4jPackV2;
import org.neo4j.function.Predicates;
import org.neo4j.function.ThrowingAction;
import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.harness.junit.EnterpriseNeo4jRule;
import org.neo4j.harness.junit.Neo4jRule;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.api.net.NetworkConnectionTracker;
import org.neo4j.kernel.api.net.TrackedNetworkConnection;
import org.neo4j.kernel.configuration.Settings;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.values.storable.Value;

import static java.time.format.DateTimeFormatter.ISO_OFFSET_DATE_TIME;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.stream.Collectors.toList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgRecord;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.msgSuccess;
import static org.neo4j.bolt.v1.runtime.spi.StreamMatchers.eqRecord;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.auth_enabled;
import static org.neo4j.helpers.collection.Iterators.single;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.Terminated;
import static org.neo4j.kernel.impl.enterprise.configuration.OnlineBackupSettings.online_backup_enabled;
import static org.neo4j.net.ConnectionTrackingIT.TestConnector.BOLT;
import static org.neo4j.net.ConnectionTrackingIT.TestConnector.HTTP;
import static org.neo4j.net.ConnectionTrackingIT.TestConnector.HTTPS;
import static org.neo4j.server.configuration.ServerSettings.webserver_max_threads;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.server.HTTP.RawPayload;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;
import static org.neo4j.test.server.HTTP.RawPayload.rawPayload;
import static org.neo4j.test.server.HTTP.Response;
import static org.neo4j.test.server.HTTP.withBasicAuth;
import static org.neo4j.values.storable.Values.stringOrNoValue;
import static org.neo4j.values.storable.Values.stringValue;

public class ConnectionTrackingIT
{
    private static final String NEO4J_USER_PWD = "test";
    private static final String OTHER_USER = "otherUser";
    private static final String OTHER_USER_PWD = "test";

    private static final List<String> LIST_CONNECTIONS_PROCEDURE_COLUMNS = Arrays.asList(
            "connectionId", "connectTime", "connector", "username", "userAgent", "serverAddress", "clientAddress" );

    @ClassRule
    public static final Neo4jRule neo4j = new EnterpriseNeo4jRule()
            .withConfig( auth_enabled, "true" )
            .withConfig( "dbms.connector.https.enabled", "true" )
            .withConfig( webserver_max_threads, "50" ) // higher than the amount of concurrent requests tests execute
            .withConfig( online_backup_enabled, Settings.FALSE );

    private static long dummyNodeId;

    private final ExecutorService executor = Executors.newCachedThreadPool();
    private final Set<TransportConnection> connections = ConcurrentHashMap.newKeySet();
    private final TransportTestUtil util = new TransportTestUtil( new Neo4jPackV2() );

    @BeforeClass
    public static void beforeAll()
    {
        changeDefaultPasswordForUserNeo4j( NEO4J_USER_PWD );
        createNewUser( OTHER_USER, OTHER_USER_PWD );
        dummyNodeId = createDummyNode();
    }

    @After
    public void afterEach() throws Exception
    {
        for ( TransportConnection connection : connections )
        {
            try
            {
                connection.disconnect();
            }
            catch ( Exception ignore )
            {
            }
        }
        for ( TrackedNetworkConnection connection : acceptedConnectionsFromConnectionTracker() )
        {
            try
            {
                connection.close();
            }
            catch ( Exception ignore )
            {
            }
        }
        executor.shutdownNow();
        terminateAllTransactions();
        awaitNumberOfAcceptedConnectionsToBe( 0 );
    }

    @Test
    public void shouldListNoConnectionsWhenIdle() throws Exception
    {
        verifyConnectionCount( HTTP, null, 0 );
        verifyConnectionCount( HTTPS, null, 0 );
        verifyConnectionCount( BOLT, null, 0 );
    }

    @Test
    public void shouldListUnauthenticatedHttpConnections() throws Exception
    {
        testListingOfUnauthenticatedConnections( 5, 0, 0 );
    }

    @Test
    public void shouldListUnauthenticatedHttpsConnections() throws Exception
    {
        testListingOfUnauthenticatedConnections( 0, 2, 0 );
    }

    @Test
    public void shouldListUnauthenticatedBoltConnections() throws Exception
    {
        testListingOfUnauthenticatedConnections( 0, 0, 4 );
    }

    @Test
    public void shouldListUnauthenticatedConnections() throws Exception
    {
        testListingOfUnauthenticatedConnections( 3, 2, 7 );
    }

    @Test
    public void shouldListAuthenticatedHttpConnections() throws Exception
    {
        lockNodeAndExecute( dummyNodeId, () ->
        {
            for ( int i = 0; i < 4; i++ )
            {
                updateNodeViaHttp( dummyNodeId, "neo4j", NEO4J_USER_PWD );
            }
            for ( int i = 0; i < 3; i++ )
            {
                updateNodeViaHttp( dummyNodeId, OTHER_USER, OTHER_USER_PWD );
            }

            awaitNumberOfAuthenticatedConnectionsToBe( 7 );
            verifyAuthenticatedConnectionCount( HTTP, "neo4j", 4 );
            verifyAuthenticatedConnectionCount( HTTP, OTHER_USER, 3 );
        } );
    }

    @Test
    public void shouldListAuthenticatedHttpsConnections() throws Exception
    {
        lockNodeAndExecute( dummyNodeId, () ->
        {
            for ( int i = 0; i < 4; i++ )
            {
                updateNodeViaHttps( dummyNodeId, "neo4j", NEO4J_USER_PWD );
            }
            for ( int i = 0; i < 5; i++ )
            {
                updateNodeViaHttps( dummyNodeId, OTHER_USER, OTHER_USER_PWD );
            }

            awaitNumberOfAuthenticatedConnectionsToBe( 9 );
            verifyAuthenticatedConnectionCount( HTTPS, "neo4j", 4 );
            verifyAuthenticatedConnectionCount( HTTPS, OTHER_USER, 5 );
        } );
    }

    @Test
    public void shouldListAuthenticatedBoltConnections() throws Exception
    {
        lockNodeAndExecute( dummyNodeId, () ->
        {
            for ( int i = 0; i < 2; i++ )
            {
                updateNodeViaBolt( dummyNodeId, "neo4j", NEO4J_USER_PWD );
            }
            for ( int i = 0; i < 5; i++ )
            {
                updateNodeViaBolt( dummyNodeId, OTHER_USER, OTHER_USER_PWD );
            }

            awaitNumberOfAuthenticatedConnectionsToBe( 7 );
            verifyAuthenticatedConnectionCount( BOLT, "neo4j", 2 );
            verifyAuthenticatedConnectionCount( BOLT, OTHER_USER, 5 );
        } );
    }

    @Test
    public void shouldListAuthenticatedConnections() throws Exception
    {
        lockNodeAndExecute( dummyNodeId, () ->
        {
            for ( int i = 0; i < 4; i++ )
            {
                updateNodeViaBolt( dummyNodeId, OTHER_USER, OTHER_USER_PWD );
            }
            for ( int i = 0; i < 1; i++ )
            {
                updateNodeViaHttp( dummyNodeId, "neo4j", NEO4J_USER_PWD );
            }
            for ( int i = 0; i < 5; i++ )
            {
                updateNodeViaHttps( dummyNodeId, "neo4j", NEO4J_USER_PWD );
            }

            awaitNumberOfAuthenticatedConnectionsToBe( 10 );
            verifyConnectionCount( BOLT, OTHER_USER, 4 );
            verifyConnectionCount( HTTP, "neo4j", 1 );
            verifyConnectionCount( HTTPS, "neo4j", 5 );
        } );
    }

    @Test
    public void shouldKillHttpConnection() throws Exception
    {
        testKillingOfConnections( neo4j.httpURI(), HTTP, 4 );
    }

    @Test
    public void shouldKillHttpsConnection() throws Exception
    {
        testKillingOfConnections( neo4j.httpsURI(), HTTPS, 2 );
    }

    @Test
    public void shouldKillBoltConnection() throws Exception
    {
        testKillingOfConnections( neo4j.boltURI(), BOLT, 3 );
    }

    private void testListingOfUnauthenticatedConnections( int httpCount, int httpsCount, int boltCount ) throws Exception
    {
        for ( int i = 0; i < httpCount; i++ )
        {
            connectSocketTo( neo4j.httpURI() );
        }

        for ( int i = 0; i < httpsCount; i++ )
        {
            connectSocketTo( neo4j.httpsURI() );
        }

        for ( int i = 0; i < boltCount; i++ )
        {
            connectSocketTo( neo4j.boltURI() );
        }

        awaitNumberOfAcceptedConnectionsToBe( httpCount + httpsCount + boltCount );

        verifyConnectionCount( HTTP, null, httpCount );
        verifyConnectionCount( HTTPS, null, httpsCount );
        verifyConnectionCount( BOLT, null, boltCount );
    }

    private void testKillingOfConnections( URI uri, TestConnector connector, int count ) throws Exception
    {
        List<TransportConnection> socketConnections = new ArrayList<>();
        for ( int i = 0; i < count; i++ )
        {
            socketConnections.add( connectSocketTo( uri ) );
        }

        awaitNumberOfAcceptedConnectionsToBe( count );
        verifyConnectionCount( connector, null, count );

        killAcceptedConnectionViaBolt();
        verifyConnectionCount( connector, null, 0 );

        for ( TransportConnection socketConnection : socketConnections )
        {
            assertConnectionBreaks( socketConnection );
        }
    }

    private TransportConnection connectSocketTo( URI uri ) throws IOException
    {
        SocketConnection connection = new SocketConnection();
        connections.add( connection );
        connection.connect( new HostnamePort( uri.getHost(), uri.getPort() ) );
        return connection;
    }

    private static void awaitNumberOfAuthenticatedConnectionsToBe( int n ) throws InterruptedException
    {
        assertEventually( "Unexpected number of authenticated connections",
                ConnectionTrackingIT::authenticatedConnectionsFromConnectionTracker, hasSize( n ),
                1, MINUTES );
    }

    private static void awaitNumberOfAcceptedConnectionsToBe( int n ) throws InterruptedException
    {
        assertEventually( connections -> "Unexpected number of accepted connections: " + connections,
                ConnectionTrackingIT::acceptedConnectionsFromConnectionTracker, hasSize( n ),
                1, MINUTES );
    }

    private static void verifyConnectionCount( TestConnector connector, String username, int expectedCount ) throws InterruptedException
    {
        verifyConnectionCount( connector, username, expectedCount, false );
    }

    private static void verifyAuthenticatedConnectionCount( TestConnector connector, String username, int expectedCount ) throws InterruptedException
    {
        verifyConnectionCount( connector, username, expectedCount, true );
    }

    private static void verifyConnectionCount( TestConnector connector, String username, int expectedCount, boolean expectAuthenticated )
            throws InterruptedException
    {
        assertEventually( connections -> "Unexpected number of listed connections: " + connections,
                () -> listMatchingConnection( connector, username, expectAuthenticated ), hasSize( expectedCount ),
                1, MINUTES );
    }

    private static List<Map<String,Object>> listMatchingConnection( TestConnector connector, String username, boolean expectAuthenticated )
    {
        Result result = neo4j.getGraphDatabaseService().execute( "CALL dbms.listConnections()" );
        assertEquals( LIST_CONNECTIONS_PROCEDURE_COLUMNS, result.columns() );
        List<Map<String,Object>> records = result.stream().collect( toList() );

        List<Map<String,Object>> matchingRecords = new ArrayList<>();
        for ( Map<String,Object> record : records )
        {
            String actualConnector = record.get( "connector" ).toString();
            assertNotNull( actualConnector );
            Object actualUsername = record.get( "username" );
            if ( Objects.equals( connector.name, actualConnector ) && Objects.equals( username, actualUsername ) )
            {
                if ( expectAuthenticated )
                {
                    assertEquals( connector.userAgent, record.get( "userAgent" ) );
                }

                matchingRecords.add( record );
            }

            assertThat( record.get( "connectionId" ).toString(), startsWith( actualConnector ) );
            OffsetDateTime connectTime = ISO_OFFSET_DATE_TIME.parse( record.get( "connectTime" ).toString(), OffsetDateTime::from );
            assertNotNull( connectTime );
            assertThat( record.get( "serverAddress" ), instanceOf( String.class ) );
            assertThat( record.get( "clientAddress" ), instanceOf( String.class ) );
        }
        return matchingRecords;
    }

    private static List<TrackedNetworkConnection> authenticatedConnectionsFromConnectionTracker()
    {
        return acceptedConnectionsFromConnectionTracker().stream()
                .filter( connection -> connection.username() != null )
                .collect( toList() );
    }

    private static List<TrackedNetworkConnection> acceptedConnectionsFromConnectionTracker()
    {
        GraphDatabaseAPI db = (GraphDatabaseAPI) neo4j.getGraphDatabaseService();
        NetworkConnectionTracker connectionTracker = db.getDependencyResolver().resolveDependency( NetworkConnectionTracker.class );
        return connectionTracker.activeConnections();
    }

    private static void changeDefaultPasswordForUserNeo4j( String newPassword )
    {
        String changePasswordUri = neo4j.httpURI().resolve( "user/neo4j/password" ).toString();
        Response response = withBasicAuth( "neo4j", "neo4j" )
                .POST( changePasswordUri, quotedJson( "{'password':'" + newPassword + "'}" ) );

        assertEquals( 200, response.status() );
    }

    private static void createNewUser( String username, String password )
    {
        String uri = txCommitUri( false );

        Response response1 = withBasicAuth( "neo4j", NEO4J_USER_PWD )
                .POST( uri, query( "CALL dbms.security.createUser(\\\"" + username + "\\\", \\\"" + password + "\\\", false)" ) );
        assertEquals( 200, response1.status() );

        Response response2 = withBasicAuth( "neo4j", NEO4J_USER_PWD )
                .POST( uri, query( "CALL dbms.security.addRoleToUser(\\\"admin\\\", \\\"" + username + "\\\")" ) );
        assertEquals( 200, response2.status() );
    }

    private static long createDummyNode()
    {
        try ( Result result = neo4j.getGraphDatabaseService().execute( "CREATE (n:Dummy) RETURN id(n) AS i" ) )
        {
            Map<String,Object> record = single( result );
            return (long) record.get( "i" );
        }
    }

    private static void lockNodeAndExecute( long id, ThrowingAction<Exception> action ) throws Exception
    {
        GraphDatabaseService db = neo4j.getGraphDatabaseService();
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.getNodeById( id );
            Lock lock = tx.acquireWriteLock( node );
            try
            {
                action.apply();
            }
            finally
            {
                lock.release();
            }
            tx.failure();
        }
    }

    private Future<Response> updateNodeViaHttp( long id, String username, String password )
    {
        return updateNodeViaHttp( id, false, username, password );
    }

    private Future<Response> updateNodeViaHttps( long id, String username, String password )
    {
        return updateNodeViaHttp( id, true, username, password );
    }

    private Future<Response> updateNodeViaHttp( long id, boolean encrypted, String username, String password )
    {
        String uri = txCommitUri( encrypted );
        String userAgent = encrypted ? HTTPS.userAgent : HTTP.userAgent;

        return executor.submit( () ->
                withBasicAuth( username, password )
                        .withHeaders( HttpHeaders.USER_AGENT, userAgent )
                        .POST( uri, query( "MATCH (n) WHERE id(n) = " + id + " SET n.prop = 42" ) )
        );
    }

    private Future<Void> updateNodeViaBolt( long id, String username, String password )
    {
        return executor.submit( () ->
        {
            connectSocketTo( neo4j.boltURI() )
                    .send( util.defaultAcceptedVersions() )
                    .send( util.chunk( initMessage( username, password ) ) )
                    .send( util.chunk( new RunMessage( "MATCH (n) WHERE id(n) = " + id + " SET n.prop = 42" ), PullAllMessage.INSTANCE ) );

            return null;
        } );
    }

    private void killAcceptedConnectionViaBolt() throws Exception
    {
        for ( TrackedNetworkConnection connection : acceptedConnectionsFromConnectionTracker() )
        {
            killConnectionViaBolt( connection );
        }
    }

    private void killConnectionViaBolt( TrackedNetworkConnection trackedConnection ) throws Exception
    {
        String id = trackedConnection.id();
        String user = trackedConnection.username();

        TransportConnection connection = connectSocketTo( neo4j.boltURI() );
        try
        {
            connection.send( util.defaultAcceptedVersions() )
                    .send( util.chunk( initMessage( "neo4j", NEO4J_USER_PWD ) ) )
                    .send( util.chunk( new RunMessage( "CALL dbms.killConnection('" + id + "')" ), PullAllMessage.INSTANCE ) );

            assertThat( connection, util.eventuallyReceivesSelectedProtocolVersion() );
            assertThat( connection, util.eventuallyReceives(
                    msgSuccess(),
                    msgSuccess(),
                    msgRecord( eqRecord( any( Value.class ), equalTo( stringOrNoValue( user ) ), equalTo( stringValue( "Connection found" ) ) ) ),
                    msgSuccess() ) );
        }
        finally
        {
            connection.disconnect();
        }
    }

    private static void assertConnectionBreaks( TransportConnection connection ) throws TimeoutException
    {
        Predicates.await( () -> connectionIsBroken( connection ), 1, MINUTES );
    }

    private static boolean connectionIsBroken( TransportConnection connection )
    {
        try
        {
            connection.send( new byte[]{1} );
            connection.recv( 1 );
            return false;
        }
        catch ( SocketException e )
        {
            return true;
        }
        catch ( IOException e )
        {
            return false;
        }
        catch ( InterruptedException e )
        {
            Thread.currentThread().interrupt();
            throw new RuntimeException( e );
        }
    }

    private static void terminateAllTransactions()
    {
        DependencyResolver dependencyResolver = ((GraphDatabaseAPI) neo4j.getGraphDatabaseService()).getDependencyResolver();
        KernelTransactions kernelTransactions = dependencyResolver.resolveDependency( KernelTransactions.class );
        kernelTransactions.activeTransactions().forEach( h -> h.markForTermination( Terminated ) );
    }

    private static String txCommitUri( boolean encrypted )
    {
        URI baseUri = encrypted ? neo4j.httpsURI() : neo4j.httpURI();
        return baseUri.resolve( "db/data/transaction/commit" ).toString();
    }

    private static RawPayload query( String statement )
    {
        return rawPayload( "{\"statements\":[{\"statement\":\"" + statement + "\"}]}" );
    }

    private static InitMessage initMessage( String username, String password )
    {
        Map<String,Object> authToken = map( "scheme", "basic", "principal", username, "credentials", password );
        return new InitMessage( BOLT.userAgent, authToken );
    }

    enum TestConnector
    {
        HTTP( "http", "http-user-agent" ),
        HTTPS( "https", "https-user-agent" ),
        BOLT( "bolt", "bolt-user-agent" );

        final String name;
        final String userAgent;

        TestConnector( String name, String userAgent )
        {
            this.name = name;
            this.userAgent = userAgent;
        }
    }
}
