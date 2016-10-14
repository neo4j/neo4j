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
package org.neo4j.coreedge.scenarios;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.logging.Level;

import org.neo4j.coreedge.core.CoreEdgeClusterSettings;
import org.neo4j.coreedge.core.consensus.roles.Role;
import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.discovery.ClusterMember;
import org.neo4j.coreedge.discovery.CoreClusterMember;
import org.neo4j.coreedge.discovery.EdgeClusterMember;
import org.neo4j.driver.internal.NetworkSession;
import org.neo4j.driver.internal.RoutingNetworkSession;
import org.neo4j.driver.internal.logging.JULogging;
import org.neo4j.driver.internal.net.BoltServerAddress;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.io.fs.FileUtils;
import org.neo4j.test.coreedge.ClusterRule;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class BoltCoreEdgeIT
{
    private static final long DEFAULT_TIMEOUT_MS = 15_000;
    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() ).withNumberOfCoreMembers( 3 );

    private Cluster cluster;

    @Before
    public void setup() throws Exception
    {
        File knownHosts = new File( System.getProperty( "user.home" ) + "/.neo4j/known_hosts" );
        FileUtils.deleteFile( knownHosts );
    }

    @Test
    public void shouldExecuteReadAndWritesWhenDriverSuppliedWithAddressOfLeader() throws Exception
    {
        // given
        cluster = clusterRule.withNumberOfEdgeMembers( 0 ).startCluster();
        cluster.coreTx( ( db, tx ) ->
        {
            Iterators.count( db.execute( "CREATE CONSTRAINT ON (p:Person) ASSERT p.name is UNIQUE" ) );
            tx.success();
        } );

        // when
        int count = executeWriteAndReadThroughBolt( cluster.awaitLeader() );

        // then
        assertEquals( 1, count );
    }

    @Test
    public void shouldExecuteReadAndWritesWhenDriverSuppliedWithAddressOfFollower() throws Exception
    {
        // given
        cluster = clusterRule.withNumberOfEdgeMembers( 0 ).startCluster();
        cluster.coreTx( ( db, tx ) ->
        {
            Iterators.count( db.execute( "CREATE CONSTRAINT ON (p:Person) ASSERT p.name is UNIQUE" ) );
            tx.success();
        } );

        // when
        int count = executeWriteAndReadThroughBolt( cluster.getDbWithRole( Role.FOLLOWER ) );

        // then
        assertEquals( 1, count );
    }

    private int executeWriteAndReadThroughBolt( CoreClusterMember core ) throws TimeoutException, InterruptedException
    {
        try ( Driver driver = GraphDatabase.driver( core.routingURI(), AuthTokens.basic( "neo4j", "neo4j" ) ) )
        {
            int count = inExpirableSession( driver, ( d ) -> d.session( AccessMode.WRITE ), ( session ) ->
            {
                // when
                session.run( "MERGE (n:Person {name: 'Jim'})" ).consume();
                Record record = session.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                return record.get( "count" ).asInt();
            } );

            return count;
        }
    }

    @Test
    public void shouldNotBeAbleToWriteOnAReadSession() throws Exception
    {
        // given
        cluster = clusterRule.withNumberOfEdgeMembers( 0 ).startCluster();

        assertEventually( "Failed to execute write query on read server", () ->
        {
            triggerElection();
            CoreClusterMember leader = cluster.awaitLeader();
            Driver driver = GraphDatabase.driver( leader.routingURI(), AuthTokens.basic( "neo4j", "neo4j" ) );

            try ( Session session = driver.session( AccessMode.READ ) )
            {
                // when
                session.run( "CREATE (n:Person {name: 'Jim'})" ).consume();
                return false;
            }
            catch ( ClientException ex )
            {
                assertEquals( "Write queries cannot be performed in READ access mode.", ex.getMessage() );
                return true;
            }
            finally
            {
                driver.close();
            }
        }, is( true ), 30, SECONDS );
    }

    @Test
    public void sessionShouldExpireOnLeaderSwitch() throws Exception
    {
        // given
        cluster = clusterRule.withNumberOfEdgeMembers( 0 ).startCluster();

        CoreClusterMember leader = cluster.awaitLeader();

        Driver driver = GraphDatabase.driver( leader.routingURI(), AuthTokens.basic( "neo4j", "neo4j" ) );
        try ( Session session = driver.session() )
        {
            session.run( "CREATE (n:Person {name: 'Jim'})" ).consume();

            // when
            switchLeader( leader );

            session.run( "CREATE (n:Person {name: 'Mark'})" ).consume();

            fail( "Should have thrown exception" );
        }
        catch ( SessionExpiredException sep )
        {
            // then
            assertEquals( String.format( "Server at %s no longer accepts writes", leader.boltAdvertisedAddress() ),
                    sep.getMessage() );
        }
        finally
        {
            driver.close();
        }
    }

    @Test
    public void shouldPickANewServerToWriteToOnLeaderSwitch() throws Throwable
    {
        // given
        cluster = clusterRule.withNumberOfEdgeMembers( 0 ).startCluster();

        CoreClusterMember leader = cluster.awaitLeader();

        CountDownLatch startTheLeaderSwitching = new CountDownLatch( 1 );

        Thread thread = new Thread( () ->
        {
            try
            {
                startTheLeaderSwitching.await( DEFAULT_TIMEOUT_MS, TimeUnit.MILLISECONDS );
                CoreClusterMember theLeader = cluster.awaitLeader();
                switchLeader( theLeader );
            }
            catch ( TimeoutException | InterruptedException e )
            {
                // ignore
            }
        } );

        thread.start();

        Config config = Config.build().withLogging( new JULogging( Level.OFF ) ).toConfig();
        try ( Driver driver = GraphDatabase
                .driver( leader.routingURI(), AuthTokens.basic( "neo4j", "neo4j" ), config ) )
        {
            boolean success = false;
            Set<BoltServerAddress> seenAddresses = new HashSet<>();

            long deadline = System.currentTimeMillis() + (30 * 1000);

            while ( !success )
            {
                if ( System.currentTimeMillis() > deadline )
                {
                    fail( "Failed to write to the new leader in time" );
                }

                try ( Session session = driver.session( AccessMode.WRITE ) )
                {
                    BoltServerAddress boltServerAddress = ((RoutingNetworkSession) session).address();

                    seenAddresses.add( boltServerAddress );
                    session.run( "CREATE (p:Person)" );
                    success = seenAddresses.size() >= 2;
                }
                catch ( Exception e )
                {
                    Thread.sleep( 100 );
                }

                /*
                 * Having the latch release here ensures that we've done at least one pass through the loop, which means
                 * we've completed a connection before the forced master switch.
                 */
                startTheLeaderSwitching.countDown();
            }
        }
        finally
        {
            thread.join();
        }
    }

    @Test
    public void sessionCreationShouldFailIfCallingDiscoveryProcedureOnEdgeServer() throws Exception
    {
        // given
        cluster = clusterRule.withNumberOfEdgeMembers( 1 ).startCluster();

        EdgeClusterMember edgeServer = cluster.getEdgeMemberById( 0 );
        try
        {
            GraphDatabase.driver( edgeServer.routingURI(), AuthTokens.basic( "neo4j", "neo4j" ) );
            fail( "Should have thrown an exception using an edge address for routing" );
        }
        catch ( ServiceUnavailableException ex )
        {
            // then
            assertEquals( format( "Server %s couldn't perform discovery", edgeServer.boltAdvertisedAddress() ),
                    ex.getMessage() );
        }
    }

    @Test
    public void sessionShouldExpireOnFailingReadQuery() throws Exception
    {
        // given
        cluster = clusterRule.withNumberOfEdgeMembers( 1 ).startCluster();
        CoreClusterMember coreServer = cluster.getCoreMemberById( 0 );

        Driver driver = GraphDatabase.driver( coreServer.routingURI(), AuthTokens.basic( "neo4j", "neo4j" ) );

        inExpirableSession( driver, Driver::session, ( session ) ->
        {
            session.run( "CREATE (p:Person {name: {name} })", Values.parameters( "name", "Jim" ) );
            return null;
        } );

        try ( Session readSession = driver.session( AccessMode.READ ) )
        {
            // when
            connectedServer( readSession ).shutdown();

            // then
            readSession.run( "MATCH (n) RETURN n LIMIT 1" ).consume();
            fail( "Should have thrown an exception as the edge server went away mid query" );
        }
        catch ( SessionExpiredException sep )
        {
            // then
            assertEquals( String.format( "Server at %s is no longer available", coreServer.boltAdvertisedAddress() ),
                    sep.getMessage() );
        }
        finally
        {
            driver.close();
        }
    }

    /*
       Create a session with empty arg list (no AccessMode arg), in a driver that was initialized with a bolt+routing
       URI, and ensure that it
       a) works against the Leader for reads and writes before a leader switch, and
       b) receives a SESSION EXPIRED after a leader switch, and
       c) keeps working if a new session is created after that exception, again with no access mode specified.
     */
    @Test
    public void shouldReadAndWriteToANewSessionCreatedAfterALeaderSwitch() throws Exception
    {
        // given
        cluster = clusterRule.withNumberOfEdgeMembers( 1 ).startCluster();
        CoreClusterMember leader = cluster.awaitLeader();

        try ( Driver driver = GraphDatabase.driver( leader.routingURI(), AuthTokens.basic( "neo4j", "neo4j" ) ) )
        {
            inExpirableSession( driver, Driver::session, ( session ) ->
            {
                // execute a write/read query
                session.run( "CREATE (p:Person {name: {name} })", Values.parameters( "name", "Jim" ) );
                Record record = session.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                assertEquals( 1, record.get( "count" ).asInt() );

                // change leader
                switchLeader( leader );

                try
                {
                    session.run( "CREATE (p:Person {name: {name} })" ).consume();
                    fail( "Should have thrown an exception as the leader went away mid session" );
                }
                catch ( SessionExpiredException sep )
                {
                    // then
                    assertEquals(
                            String.format( "Server at %s no longer accepts writes", leader.boltAdvertisedAddress() ),
                            sep.getMessage() );
                }
                return null;
            } );

            inExpirableSession( driver, Driver::session, ( session ) ->
            {
                // execute a write/read query
                session.run( "CREATE (p:Person {name: {name} })", Values.parameters( "name", "Jim" ) );
                Record record = session.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                assertEquals( 2, record.get( "count" ).asInt() );
                return null;
            } );
        }
    }

    // Ensure that Bookmarks work with single instances using a driver created using a bolt[not+routing] URI.
    @Test
    public void bookmarksShouldWorkWithDriverPinnedToSingleServer() throws Exception
    {
        // given
        cluster = clusterRule.withNumberOfEdgeMembers( 1 ).startCluster();
        CoreClusterMember leader = cluster.awaitLeader();

        try ( Driver driver = GraphDatabase.driver( leader.directURI(), AuthTokens.basic( "neo4j", "neo4j" ) ) )
        {
            String bookmark = inExpirableSession( driver, Driver::session, ( session ) ->
            {
                try ( Transaction tx = session.beginTransaction() )
                {
                    tx.run( "CREATE (p:Person {name: {name} })", Values.parameters( "name", "Alistair" ) );
                    tx.success();
                }

                return session.lastBookmark();
            } );

            assertNotNull( bookmark );

            try ( Session session = driver.session(); Transaction tx = session.beginTransaction( bookmark ) )
            {
                Record record = tx.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                assertEquals( 1, record.get( "count" ).asInt() );
                tx.success();
            }
        }
    }

    @Test
    public void shouldUseBookmarkFromAReadSessionInAWriteSession() throws Exception
    {
        // given
        cluster = clusterRule.withNumberOfEdgeMembers( 1 ).startCluster();
        CoreClusterMember leader = cluster.awaitLeader();

        try ( Driver driver = GraphDatabase.driver( leader.directURI(), AuthTokens.basic( "neo4j", "neo4j" ) ) )
        {
            inExpirableSession( driver, ( d ) -> d.session( AccessMode.WRITE ), ( session ) ->
            {
                session.run( "CREATE (p:Person {name: {name} })", Values.parameters( "name", "Jim" ) );
                return null;
            } );

            String bookmark;
            try ( Session session = driver.session( AccessMode.READ ) )
            {
                try ( Transaction tx = session.beginTransaction() )
                {
                    tx.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                    tx.success();
                }

                bookmark = session.lastBookmark();
            }

            assertNotNull( bookmark );

            inExpirableSession( driver, ( d ) -> d.session( AccessMode.WRITE ), ( session ) ->
            {
                try ( Transaction tx = session.beginTransaction( bookmark ) )
                {
                    tx.run( "CREATE (p:Person {name: {name} })", Values.parameters( "name", "Alistair" ) );
                    tx.success();
                }

                return null;
            } );

            try ( Session session = driver.session() )
            {
                Record record = session.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                assertEquals( 2, record.get( "count" ).asInt() );
            }
        }
    }

    @Test
    public void shouldUseBookmarkFromAWriteSessionInAReadSession() throws Exception
    {
        // given
        cluster = clusterRule.withNumberOfEdgeMembers( 1 ).startCluster();

        CoreClusterMember leader = cluster.awaitLeader();
        EdgeClusterMember edgeMember = cluster.getEdgeMemberById( 0 );

        edgeMember.txPollingClient().pause();

        Driver driver = GraphDatabase.driver( leader.directURI(), AuthTokens.basic( "neo4j", "neo4j" ) );

        String bookmark = inExpirableSession( driver, ( d ) -> d.session( AccessMode.WRITE ), ( session ) ->
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                tx.run( "CREATE (p:Person {name: {name} })", Values.parameters( "name", "Jim" ) );
                tx.run( "CREATE (p:Person {name: {name} })", Values.parameters( "name", "Alistair" ) );
                tx.run( "CREATE (p:Person {name: {name} })", Values.parameters( "name", "Mark" ) );
                tx.run( "CREATE (p:Person {name: {name} })", Values.parameters( "name", "Chris" ) );
                tx.success();
            }

            return session.lastBookmark();
        } );

        assertNotNull( bookmark );
        edgeMember.txPollingClient().resume();

        driver = GraphDatabase.driver( edgeMember.directURI(), AuthTokens.basic( "neo4j", "neo4j" ) );

        try ( Session session = driver.session( AccessMode.READ ) )
        {
            try ( Transaction tx = session.beginTransaction( bookmark ) )
            {
                Record record = tx.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                tx.success();
                assertEquals( 4, record.get( "count" ).asInt() );
            }
        }
    }

    @Test
    public void shouldSendRequestsToNewlyAddedServers() throws Throwable
    {
        // given
        cluster = clusterRule.withNumberOfEdgeMembers( 1 )
                .withSharedCoreParams( stringMap( CoreEdgeClusterSettings.cluster_routing_ttl.name(), "1s" ) )
                .startCluster();

        CoreClusterMember leader = cluster.awaitLeader();
        Driver driver = GraphDatabase.driver( leader.routingURI(), AuthTokens.basic( "neo4j", "neo4j" ) );

        String bookmark = inExpirableSession( driver, ( d ) -> d.session( AccessMode.WRITE ), ( session ) ->
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                tx.run( "CREATE (p:Person {name: {name} })", Values.parameters( "name", "Jim" ) );
                tx.success();
            }

            return session.lastBookmark();
        } );

        // when
        Set<String> unusedServers = new HashSet<>();

        for ( CoreClusterMember coreClusterMember : cluster.coreMembers() )
        {
            unusedServers.add( coreClusterMember.boltAdvertisedAddress() );
        }
        for ( EdgeClusterMember edgeClusterMember : cluster.edgeMembers() )
        {
            unusedServers.add( edgeClusterMember.boltAdvertisedAddress() );
        }

        for ( int i = 1; i <= 3; i++ )
        {
            EdgeClusterMember newEdge = cluster.addEdgeMemberWithId( i );
            unusedServers.add( newEdge.boltAdvertisedAddress() );
            newEdge.start();
        }

        assertEventually( "Failed to send requests to all servers", () ->
        {
            for ( int i = 0; i < cluster.coreMembers().size() + cluster.edgeMembers().size(); i++ )
            {
                try ( Session session = driver.session( AccessMode.READ ) )
                {
                    BoltServerAddress boltServerAddress = ((RoutingNetworkSession) session).address();
                    executeReadQuery( bookmark, session );
                    unusedServers.remove( boltServerAddress.toString() );
                }
                catch ( Throwable throwable )
                {
                    return false;
                }
            }

            return unusedServers.size() == 0;
        }, is( true ), 30, SECONDS );
    }

    private void executeReadQuery( String bookmark, Session session )
    {
        try ( Transaction tx = session.beginTransaction( bookmark ) )
        {
            Record record = tx.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
            assertEquals( 1, record.get( "count" ).asInt() );
        }
    }

    private <T> T inExpirableSession( Driver driver, Function<Driver,Session> acquirer, Function<Session,T> op )
            throws TimeoutException, InterruptedException
    {
        long endTime = System.currentTimeMillis() + DEFAULT_TIMEOUT_MS;

        do
        {
            try ( Session session = acquirer.apply( driver ) )
            {
                return op.apply( session );
            }
            catch ( SessionExpiredException e )
            {
                // role might have changed; try again;
            }
        }
        while ( System.currentTimeMillis() < endTime );

        throw new TimeoutException( "Transaction did not succeed in time" );
    }

    private ClusterMember connectedServer( Session session ) throws NoSuchFieldException, IllegalAccessException
    {
        Field connectionField = NetworkSession.class.getDeclaredField( "connection" );
        connectionField.setAccessible( true );
        Connection connection = (Connection) connectionField.get( session );

        String host = connection.address().host();
        int port = connection.address().port();

        return cluster.getMemberByBoltAddress( new AdvertisedSocketAddress( host, port ) );
    }

    private void switchLeader( CoreClusterMember initialLeader )
    {
        long deadline = System.currentTimeMillis() + (30 * 1000);

        while ( initialLeader.database().getRole() != Role.FOLLOWER )
        {
            if ( System.currentTimeMillis() > deadline )
            {
                throw new RuntimeException( "Failed to switch leader in time" );
            }

            try
            {
                triggerElection();
            }
            catch ( IOException | TimeoutException e )
            {
                // keep trying
            }
        }
    }

    private void triggerElection() throws IOException, TimeoutException
    {
        CoreClusterMember aFollower = cluster.getDbWithRole( Role.FOLLOWER );

        if ( aFollower != null )
        {
            aFollower.raft().triggerElection();
            cluster.awaitLeader();
        }
    }
}
