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
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.coreedge.core.consensus.roles.Role;
import org.neo4j.coreedge.discovery.Cluster;
import org.neo4j.coreedge.discovery.ClusterMember;
import org.neo4j.coreedge.discovery.CoreClusterMember;
import org.neo4j.coreedge.discovery.EdgeClusterMember;
import org.neo4j.driver.internal.NetworkSession;
import org.neo4j.driver.internal.spi.Connection;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.AuthTokens;
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
import org.neo4j.io.fs.FileUtils;
import org.neo4j.test.coreedge.ClusterRule;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import static org.neo4j.test.assertion.Assert.assertEventually;

public class BoltCoreEdgeIT
{
    private static final long DEFAULT_TIMEOUT_MS = 15_000;
    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() )
            .withNumberOfCoreMembers( 3 )
            .withNumberOfEdgeMembers( 0 );

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
        cluster = clusterRule.startCluster();

        CoreClusterMember leader = cluster.awaitLeader();

        try ( Driver driver = GraphDatabase.driver( leader.routingURI(), AuthTokens.basic( "neo4j", "neo4j" ) ) )
        {
            inExpirableSession( driver, ( d ) -> d.session( AccessMode.WRITE ), ( session ) ->
            {
                // when
                session.run( "CREATE CONSTRAINT ON (p:Person) ASSERT p.name is UNIQUE" ).consume();
                session.run( "MERGE (n:Person {name: 'Jim'})-[:BOOM]->(m)" ).consume();

                Record record = session.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();

                // then
                assertEquals( 1, record.get( "count" ).asInt() );
                return null;
            } );
        }
    }

    @Test
    public void shouldExecuteReadAndWritesWhenDriverSuppliedWithAddressOfFollower() throws Exception
    {
        // given
        cluster = clusterRule.startCluster();

        CoreClusterMember follower = cluster.getDbWithRole(Role.FOLLOWER);

        try ( Driver driver = GraphDatabase.driver( follower.routingURI(), AuthTokens.basic( "neo4j", "neo4j" ) ) )
        {
            inExpirableSession( driver, ( d ) -> d.session( AccessMode.WRITE ), ( session ) ->
            {
                // when
                session.run( "CREATE CONSTRAINT ON (p:Person) ASSERT p.name is UNIQUE" ).consume();
                session.run( "MERGE (n:Person {name: 'Jim'})-[:BOOM]->(m)" ).consume();

                Record record = session.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();

                // then
                assertEquals( 1, record.get( "count" ).asInt() );
                return null;
            } );
        }
    }

    @Test
    public void shouldNotBeAbleToWriteOnAReadSession() throws Exception
    {
        // given
        cluster = clusterRule.startCluster();

        assertEventually( "Failed to execute write query on read server", () ->
        {
            triggerElection();
            CoreClusterMember leader = cluster.awaitLeader();
            Driver driver = GraphDatabase.driver( leader.routingURI(), AuthTokens.basic( "neo4j", "neo4j" ) );

            try ( Session session = driver.session(AccessMode.READ) )
            {
                // when
                session.run( "CREATE (n:Person {name: 'Jim'})-[:BOOM]->(m)" ).consume();
                return false;
            }
            catch ( ClientException ex )
            {
                assertEquals( String.format( "Write queries cannot be performed in READ access mode.",
                        leader.boltAdvertisedAddress()), ex.getMessage() );
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
        cluster = clusterRule.startCluster();

        CoreClusterMember leader = cluster.awaitLeader();

        Driver driver = GraphDatabase.driver( leader.routingURI(), AuthTokens.basic( "neo4j", "neo4j" ) );
        try ( Session session = driver.session() )
        {
            session.run( "CREATE CONSTRAINT ON (p:Person) ASSERT p.name is UNIQUE" ).consume();

            // when
            switchLeader( leader );

            session.run( "MERGE (n:Person {name: 'Jim'})-[:BOOM]->(m)" ).consume();

            fail( "Should have thrown exception" );
        }
        catch ( SessionExpiredException sep )
        {
            // then
            assertEquals( String.format( "Server at %s no longer accepts writes",
                    leader.boltAdvertisedAddress() ), sep.getMessage() );
        }
        finally
        {
            driver.close();
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

        try ( Session readSession = driver.session( AccessMode.READ) )
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
            assertEquals( String.format("Server at %s is no longer available", coreServer.boltAdvertisedAddress()),
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
                    assertEquals( String.format( "Server at %s no longer accepts writes",
                            leader.boltAdvertisedAddress() ), sep.getMessage() );
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
        CoreClusterMember leader = cluster.awaitLeader(  );

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
        CoreClusterMember leader = cluster.awaitLeader(  );

        try(Driver driver = GraphDatabase.driver( leader.directURI(), AuthTokens.basic( "neo4j", "neo4j" ) ))
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

        CoreClusterMember leader = cluster.awaitLeader(  );
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

        try ( Session session = driver.session(AccessMode.READ) )
        {
            try ( Transaction tx = session.beginTransaction( bookmark ) )
            {
                Record record = tx.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                tx.success();
                assertEquals( 4, record.get( "count" ).asInt() );
            }
        }
    }

    private <T> T inExpirableSession( Driver driver, Function<Driver, Session> acquirer, Function<Session, T> op )
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

        return cluster.getMemberByBoltAddress( new AdvertisedSocketAddress( host, port ) ) ;
    }

    private void switchLeader( CoreClusterMember initialLeader )
    {
        while ( initialLeader.database().getRole() != Role.FOLLOWER )
        {
            try
            {
                triggerElection();
            }
            catch ( IOException | TimeoutException e )
            {
                fail(e.getMessage());
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
