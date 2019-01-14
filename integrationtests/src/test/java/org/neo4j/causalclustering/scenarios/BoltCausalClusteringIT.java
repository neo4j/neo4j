/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.causalclustering.scenarios;

import org.junit.Rule;
import org.junit.Test;

import java.io.IOException;
import java.time.Clock;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.logging.Level;

import org.neo4j.causalclustering.catchup.tx.CatchupPollingProcess;
import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.discovery.ReadReplica;
import org.neo4j.driver.internal.logging.JULogging;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.TransactionWork;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.exceptions.ClientException;
import org.neo4j.driver.v1.exceptions.ServiceUnavailableException;
import org.neo4j.driver.v1.exceptions.SessionExpiredException;
import org.neo4j.driver.v1.summary.ServerInfo;
import org.neo4j.graphdb.factory.GraphDatabaseSettings;
import org.neo4j.helpers.collection.Iterators;
import org.neo4j.test.causalclustering.ClusterRule;
import org.neo4j.test.rule.SuppressOutput;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.startsWith;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.driver.v1.Values.parameters;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class BoltCausalClusteringIT
{
    private static final long DEFAULT_TIMEOUT_MS = 15_000;
    @Rule
    public final ClusterRule clusterRule = new ClusterRule().withNumberOfCoreMembers( 3 );
    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();

    private Cluster cluster;

    @Test
    public void shouldExecuteReadAndWritesWhenDriverSuppliedWithAddressOfLeader() throws Exception
    {
        // given
        cluster = clusterRule.withNumberOfReadReplicas( 0 ).startCluster();
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
        cluster = clusterRule.withNumberOfReadReplicas( 0 ).startCluster();
        cluster.coreTx( ( db, tx ) ->
        {
            Iterators.count( db.execute( "CREATE CONSTRAINT ON (p:Person) ASSERT p.name is UNIQUE" ) );
            tx.success();
        } );

        // when
        int count = executeWriteAndReadThroughBolt( cluster.getMemberWithRole( Role.FOLLOWER ) );

        // then
        assertEquals( 1, count );
    }

    private int executeWriteAndReadThroughBolt( CoreClusterMember core ) throws TimeoutException
    {
        try ( Driver driver = GraphDatabase.driver( core.routingURI(), AuthTokens.basic( "neo4j", "neo4j" ) ) )
        {

            return inExpirableSession( driver, d -> d.session( AccessMode.WRITE ), session ->
            {
                // when
                session.run( "MERGE (n:Person {name: 'Jim'})" ).consume();
                Record record = session.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                return record.get( "count" ).asInt();
            } );
        }
    }

    @Test
    public void shouldNotBeAbleToWriteOnAReadSession() throws Exception
    {
        // given
        cluster = clusterRule.withNumberOfReadReplicas( 0 ).startCluster();

        assertEventually( "Failed to execute write query on read server", () ->
        {
            switchLeader( cluster.awaitLeader() );
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
        cluster = clusterRule.withNumberOfReadReplicas( 0 ).startCluster();

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
    public void shouldBeAbleToGetClusterOverview() throws Exception
    {
        // given
        cluster = clusterRule.withNumberOfReadReplicas( 0 ).startCluster();

        CoreClusterMember leader = cluster.awaitLeader();

        Driver driver = GraphDatabase.driver( leader.routingURI(), AuthTokens.basic( "neo4j", "neo4j" ) );
        try ( Session session = driver.session() )
        {
            StatementResult overview = session.run( "CALL dbms.cluster.overview" );
            assertThat(overview.list(), hasSize( 3 ));
        }
        finally
        {
            driver.close();
        }
    }

    /**
     * Keeps the leader different than the initial leader.
     */
    private class LeaderSwitcher implements Runnable
    {
        private final Cluster cluster;
        private final CountDownLatch switchCompleteLatch;
        private CoreClusterMember initialLeader;
        private CoreClusterMember currentLeader;

        private Thread thread;
        private boolean stopped;
        private Throwable throwable;

        LeaderSwitcher( Cluster cluster, CountDownLatch switchCompleteLatch )
        {
            this.cluster = cluster;
            this.switchCompleteLatch = switchCompleteLatch;
        }

        @Override
        public void run()
        {
            try
            {
                initialLeader = cluster.awaitLeader();

                while ( !stopped )
                {
                    currentLeader = cluster.awaitLeader();
                    if ( currentLeader == initialLeader )
                    {
                        switchLeader( initialLeader );
                        currentLeader = cluster.awaitLeader();
                    }
                    else
                    {
                        switchCompleteLatch.countDown();
                    }

                    Thread.sleep( 100 );
                }
            }
            catch ( Throwable e )
            {
                throwable = e;
            }
        }

        void start()
        {
            if ( thread == null )
            {
                thread = new Thread( this );
                thread.start();
            }
        }

        void stop() throws Throwable
        {
            if ( thread != null )
            {
                stopped = true;
                thread.join();
            }

            assertNoException();
        }

        boolean hadLeaderSwitch()
        {
            return currentLeader != initialLeader;
        }

        void assertNoException() throws Throwable
        {
            if ( throwable != null )
            {
                throw throwable;
            }
        }
    }

    @Test
    public void shouldPickANewServerToWriteToOnLeaderSwitch() throws Throwable
    {
        // given
        cluster = clusterRule.withNumberOfReadReplicas( 0 ).startCluster();

        CoreClusterMember leader = cluster.awaitLeader();

        CountDownLatch leaderSwitchLatch = new CountDownLatch( 1 );

        LeaderSwitcher leaderSwitcher = new LeaderSwitcher( cluster, leaderSwitchLatch );

        Config config = Config.build().withLogging( new JULogging( Level.OFF ) ).toConfig();
        Set<String> seenAddresses = new HashSet<>();
        try ( Driver driver = GraphDatabase
                .driver( leader.routingURI(), AuthTokens.basic( "neo4j", "neo4j" ), config ) )
        {
            boolean success = false;

            long deadline = System.currentTimeMillis() + (30 * 1000);

            while ( !success )
            {
                if ( System.currentTimeMillis() > deadline )
                {
                    fail( "Failed to write to the new leader in time. Addresses seen: " + seenAddresses );
                }

                try ( Session session = driver.session( AccessMode.WRITE ) )
                {
                    StatementResult result = session.run( "CREATE (p:Person)" );
                    ServerInfo server = result.summary().server();
                    seenAddresses.add( server.address());
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
                if ( !seenAddresses.isEmpty() && !success )
                {
                    leaderSwitcher.start();
                    leaderSwitchLatch.await();
                }
            }
        }
        finally
        {
            leaderSwitcher.stop();
            assertTrue( leaderSwitcher.hadLeaderSwitch() );
            assertThat( seenAddresses.size(), greaterThanOrEqualTo( 2 ) );
        }
    }

    @Test
    public void sessionCreationShouldFailIfCallingDiscoveryProcedureOnEdgeServer() throws Exception
    {
        // given
        cluster = clusterRule.withNumberOfReadReplicas( 1 ).startCluster();

        ReadReplica readReplica = cluster.getReadReplicaById( 0 );
        try
        {
            GraphDatabase.driver( readReplica.routingURI(), AuthTokens.basic( "neo4j", "neo4j" ) );
            fail( "Should have thrown an exception using a read replica address for routing" );
        }
        catch ( ServiceUnavailableException ex )
        {
            // then
            assertThat(ex.getMessage(), startsWith( "Failed to run"));
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
        cluster = clusterRule.withNumberOfReadReplicas( 1 ).startCluster();
        CoreClusterMember leader = cluster.awaitLeader();

        try ( Driver driver = GraphDatabase.driver( leader.routingURI(), AuthTokens.basic( "neo4j", "neo4j" ) ) )
        {
            inExpirableSession( driver, Driver::session, session ->
            {
                // execute a write/read query
                session.run( "CREATE (p:Person {name: {name} })", Values.parameters( "name", "Jim" ) );
                Record record = session.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                assertEquals( 1, record.get( "count" ).asInt() );

                // change leader

                try
                {
                    switchLeader( leader );
                    session.run( "CREATE (p:Person {name: {name} })", Values.parameters( "name", "Mark" ) ).consume();
                    fail( "Should have thrown an exception as the leader went away mid session" );
                }
                catch ( SessionExpiredException sep )
                {
                    // then
                    assertEquals(
                            String.format( "Server at %s no longer accepts writes", leader.boltAdvertisedAddress() ),
                            sep.getMessage() );
                }
                catch ( InterruptedException e )
                {
                    // ignored
                }
                return null;
            } );

            inExpirableSession( driver, Driver::session, session ->
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
        cluster = clusterRule.withNumberOfReadReplicas( 1 ).startCluster();
        CoreClusterMember leader = cluster.awaitLeader();

        try ( Driver driver = GraphDatabase.driver( leader.directURI(), AuthTokens.basic( "neo4j", "neo4j" ) ) )
        {
            String bookmark = inExpirableSession( driver, Driver::session, session ->
            {
                try ( Transaction tx = session.beginTransaction() )
                {
                    tx.run( "CREATE (p:Person {name: {name} })", Values.parameters( "name", "Alistair" ) );
                    tx.success();
                }

                return session.lastBookmark();
            } );

            assertNotNull( bookmark );

            try ( Session session = driver.session( bookmark ); Transaction tx = session.beginTransaction() )
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
        cluster = clusterRule.withNumberOfReadReplicas( 1 ).startCluster();
        CoreClusterMember leader = cluster.awaitLeader();

        try ( Driver driver = GraphDatabase.driver( leader.directURI(), AuthTokens.basic( "neo4j", "neo4j" ) ) )
        {
            inExpirableSession( driver, d -> d.session( AccessMode.WRITE ), session ->
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

            inExpirableSession( driver, d -> d.session( AccessMode.WRITE, bookmark ), session ->
            {
                try ( Transaction tx = session.beginTransaction() )
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
    public void shouldUseBookmarkFromAWriteSessionInAReadSession() throws Throwable
    {
        // given
        cluster = clusterRule.withNumberOfReadReplicas( 1 ).startCluster();

        CoreClusterMember leader = cluster.awaitLeader();
        ReadReplica readReplica = cluster.getReadReplicaById( 0 );

        readReplica.txPollingClient().stop();

        Driver driver = GraphDatabase.driver( leader.directURI(), AuthTokens.basic( "neo4j", "neo4j" ) );

        String bookmark = inExpirableSession( driver, d -> d.session( AccessMode.WRITE ), session ->
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
        readReplica.txPollingClient().start();

        driver = GraphDatabase.driver( readReplica.directURI(), AuthTokens.basic( "neo4j", "neo4j" ) );

        try ( Session session = driver.session( AccessMode.READ, bookmark ) )
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                Record record = tx.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                tx.success();
                assertEquals( 4, record.get( "count" ).asInt() );
            }
        }
    }

    @Test
    public void shouldSendRequestsToNewlyAddedReadReplicas() throws Throwable
    {
        // given
        cluster = clusterRule.withNumberOfReadReplicas( 1 )
                .withSharedCoreParams( stringMap( CausalClusteringSettings.cluster_routing_ttl.name(), "1s" ) )
                .startCluster();

        CoreClusterMember leader = cluster.awaitLeader();
        Driver driver = GraphDatabase.driver( leader.routingURI(), AuthTokens.basic( "neo4j", "neo4j" ) );

        String bookmark = inExpirableSession( driver, d -> d.session( AccessMode.WRITE ), session ->
        {
            try ( Transaction tx = session.beginTransaction() )
            {
                tx.run( "CREATE (p:Person {name: {name} })", Values.parameters( "name", "Jim" ) );
                tx.success();
            }

            return session.lastBookmark();
        } );

        // when
        Set<String> readReplicas = new HashSet<>();

        for ( ReadReplica readReplica : cluster.readReplicas() )
        {
            readReplicas.add( readReplica.boltAdvertisedAddress() );
        }

        for ( int i = 10; i <= 13; i++ )
        {
            ReadReplica newReadReplica = cluster.addReadReplicaWithId( i );
            readReplicas.add( newReadReplica.boltAdvertisedAddress() );
            newReadReplica.start();
        }

        assertEventually( "Failed to send requests to all servers", () ->
        {
            for ( int i = 0; i < cluster.readReplicas().size(); i++ ) // don't care about cores
            {
                try ( Session session = driver.session( AccessMode.READ, bookmark ) )
                {
                    executeReadQuery( session );

                    session.readTransaction( (TransactionWork<Void>) tx ->
                    {
                        StatementResult result = tx.run( "MATCH (n:Person) RETURN COUNT(*) AS count" );

                        assertEquals( 1, result.next().get( "count" ).asInt() );

                        readReplicas.remove( result.summary().server().address() );

                        return null;
                    } );

                }
                catch ( Throwable throwable )
                {
                    return false;
                }
            }

            return readReplicas.size() == 0; // have sent something to all replicas
        }, is( true ), 30, SECONDS );
    }

    @Test
    public void shouldHandleLeaderSwitch() throws Exception
    {
        // given
        cluster = clusterRule.startCluster();

        CoreClusterMember leader = cluster.awaitLeader();

        try ( Driver driver = GraphDatabase.driver( leader.routingURI(), AuthTokens.basic( "neo4j", "neo4j" ) ) )
        {
            // when
            try ( Session session = driver.session() )
            {
                try ( Transaction tx = session.beginTransaction() )
                {
                    switchLeader( leader );

                    tx.run( "CREATE (person:Person {name: {name}, title: {title}})",
                            parameters( "name", "Webber", "title", "Mr" ) );
                    tx.success();
                }
                catch ( SessionExpiredException ignored )
                {
                    // expected
                }
            }

            String bookmark = inExpirableSession( driver, Driver::session, s ->
            {
                try ( Transaction tx = s.beginTransaction() )
                {
                    tx.run( "CREATE (person:Person {name: {name}, title: {title}})",
                            parameters( "name", "Webber", "title", "Mr" ) );
                    tx.success();
                }
                return s.lastBookmark();
            } );

            // then
            try ( Session session = driver.session( AccessMode.READ, bookmark ) )
            {
                try ( Transaction tx = session.beginTransaction() )
                {
                    Record record = tx.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
                    tx.success();
                    assertEquals( 1, record.get( "count" ).asInt() );
                }
            }
        }
    }

    @Test
    public void transactionsShouldNotAppearOnTheReadReplicaWhilePollingIsPaused() throws Throwable
    {
        // given
        Map<String,String> params = stringMap( GraphDatabaseSettings.keep_logical_logs.name(), "keep_none",
                GraphDatabaseSettings.logical_log_rotation_threshold.name(), "1M",
                GraphDatabaseSettings.check_point_interval_time.name(), "100ms",
                CausalClusteringSettings.cluster_allow_reads_on_followers.name(), "false");

        Cluster cluster = clusterRule.withSharedCoreParams( params ).withNumberOfReadReplicas( 1 ).startCluster();

        Driver driver = GraphDatabase.driver( cluster.awaitLeader().routingURI(), AuthTokens.basic( "neo4j", "neo4j" ) );

        try ( Session session = driver.session() )
        {
            session.writeTransaction( tx ->
            {
                tx.run( "MERGE (n:Person {name: 'Jim'})" );
                return null;
            } );
        }

        ReadReplica replica = cluster.findAnyReadReplica();

        CatchupPollingProcess pollingClient = replica.database().getDependencyResolver()
                .resolveDependency( CatchupPollingProcess.class );

        pollingClient.stop();

        String lastBookmark = null;
        int iterations = 5;
        final int nodesToCreate = 20000;
        for ( int i = 0; i < iterations; i++ )
        {
            try ( Session writeSession = driver.session() )
            {
                writeSession.writeTransaction( tx ->
                {

                    tx.run( "UNWIND range(1, {nodesToCreate}) AS i CREATE (n:Person {name: 'Jim'})",
                            Values.parameters( "nodesToCreate", nodesToCreate ) );
                    return null;
                } );

                lastBookmark = writeSession.lastBookmark();
            }
        }

        // when the poller is resumed, it does make it to the read replica
        pollingClient.start();

        pollingClient.upToDateFuture().get();

        int happyCount = 0;
        int numberOfRequests = 1_000;
        for ( int i = 0; i < numberOfRequests; i++ ) // don't care about cores
        {
            try ( Session session = driver.session( lastBookmark ) )
            {
                happyCount += session.readTransaction( tx ->
                {
                    tx.run( "MATCH (n:Person) RETURN COUNT(*) AS count" );
                    return 1;
                } );
            }
        }

        assertEquals( numberOfRequests, happyCount );
    }

    private void executeReadQuery( Session session )
    {
        try ( Transaction tx = session.beginTransaction() )
        {
            Record record = tx.run( "MATCH (n:Person) RETURN COUNT(*) AS count" ).next();
            assertEquals( 1, record.get( "count" ).asInt() );
        }
    }

    private <T> T inExpirableSession( Driver driver, Function<Driver,Session> acquirer, Function<Session,T> op )
            throws TimeoutException
    {
        long endTime = System.currentTimeMillis() + DEFAULT_TIMEOUT_MS;

        do
        {
            try ( Session session = acquirer.apply( driver ) )
            {
                return op.apply( session );
            }
            catch ( SessionExpiredException | ClientException e )
            {
                // role might have changed; try again;
            }
        }
        while ( System.currentTimeMillis() < endTime );

        throw new TimeoutException( "Transaction did not succeed in time" );
    }

    private void switchLeader( CoreClusterMember initialLeader ) throws InterruptedException
    {
        long deadline = System.currentTimeMillis() + (30 * 1000);

        Role role = initialLeader.database().getRole();
        while ( role != Role.FOLLOWER )
        {
            if ( System.currentTimeMillis() > deadline )
            {
                throw new RuntimeException( "Failed to switch leader in time" );
            }

            try
            {
                triggerElection( initialLeader );
            }
            catch ( IOException | TimeoutException e )
            {
                // keep trying
            }
            finally
            {
                role = initialLeader.database().getRole();
                Thread.sleep( 100 );
            }
        }
    }

    private void triggerElection( CoreClusterMember initialLeader ) throws IOException, TimeoutException
    {
        for ( CoreClusterMember coreClusterMember : cluster.coreMembers() )
        {
            if ( !coreClusterMember.equals( initialLeader ) )
            {
                coreClusterMember.raft().triggerElection( Clock.systemUTC() );
                cluster.awaitLeader();
            }
        }
    }
}
