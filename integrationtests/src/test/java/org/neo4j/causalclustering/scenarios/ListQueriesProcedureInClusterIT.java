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
package org.neo4j.causalclustering.scenarios;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.causalclustering.ClusterRule;
import org.neo4j.test.rule.VerboseTimeout;
import org.neo4j.test.rule.concurrent.ThreadingRule;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ListQueriesProcedureInClusterIT
{
    private final ClusterRule clusterRule =
            new ClusterRule( getClass() ).withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 1 );
    private final VerboseTimeout timeout = VerboseTimeout.builder()
            .withTimeout( 1000, TimeUnit.SECONDS )
            .build();
    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( clusterRule ).around( timeout );
    private Cluster cluster;
    @Rule
    public final ThreadingRule threads = new ThreadingRule();

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule.startCluster();
    }

    @Test
    public void listQueriesWillNotIncludeQueriesFromOtherServersInCluster() throws Exception
    {
        // given
        String CORE_QUERY = "MATCH (n) SET n.number = n.number - 1";
        String REPLICA_QUERY = "MATCH (n) RETURN n";
        CountDownLatch resourceLocked = new CountDownLatch( 1 );
        CountDownLatch listQueriesLatchOnCore = new CountDownLatch( 1 );
        CountDownLatch executedCoreQueryLatch = new CountDownLatch( 1 );
        ReadReplicaGraphDatabase replicaDb = cluster.findAnyReadReplica().database();

        final Node[] node = new Node[1];

        //Given
        CoreClusterMember leader = cluster.coreTx( ( CoreGraphDatabase leaderDb, Transaction tx ) ->
        {
            node[0] = leaderDb.createNode();
            tx.success();
            tx.close();
        } );

        CoreGraphDatabase leaderDb = leader.database();
        Result matchAllResult = leaderDb.execute( "MATCH (n) RETURN n" );
        assertTrue( matchAllResult.hasNext() );
        matchAllResult.close();

        try
        {
            acquireLocksAndSetupCountdownLatch( resourceLocked, listQueriesLatchOnCore, node[0] );

            resourceLocked.await();

            //When
            executeQueryOnReplicaAndLeader( leaderDb, CORE_QUERY, replicaDb, REPLICA_QUERY, executedCoreQueryLatch );

            //Then
            try
            {
                Map<String,Object> coreQueryListing = getQueryListing( CORE_QUERY, leaderDb ).get();
                Optional<Map<String,Object>> replicaQueryListing = getQueryListing( REPLICA_QUERY, replicaDb );

                assertNotNull( coreQueryListing );
                assertThat( coreQueryListing.get( "activeLockCount" ), is( 1L ) );
                assertFalse( replicaQueryListing.isPresent() );

                listQueriesLatchOnCore.countDown();
                executedCoreQueryLatch.await();

                assertFalse( getQueryListing( CORE_QUERY, leaderDb ).isPresent() );
            }
            catch ( Exception e )
            {
                e.printStackTrace();
                fail( "Couldn't countdown listqueries latch" );
            }
        }
        catch ( InterruptedException e )
        {
            e.printStackTrace();
            fail( "Should create node and list queries" );
        }
    }

    private void acquireLocksAndSetupCountdownLatch(
            CountDownLatch resourceLocked, CountDownLatch listQueriesLatch, Node node )
    {
        threads.execute( param ->
        {
            cluster.coreTx( ( leaderDb, secondTransaction ) ->
            {

                //lock node
                secondTransaction.acquireWriteLock( node );
                resourceLocked.countDown();
                try
                {
                    listQueriesLatch.await();
                }
                catch ( InterruptedException e1 )
                {
                    e1.printStackTrace();
                    fail( "failure in locking node" );
                }
            } );
            return null;
        }, null );
    }

    private void executeQueryOnReplicaAndLeader(
            CoreGraphDatabase leaderDb,
            String CORE_QUERY,
            ReadReplicaGraphDatabase replicaDb,
            String REPLICA_QUERY,
            CountDownLatch executedCoreQueryLatch )
    {
        //Execute query on Leader
        threads.execute( parameter ->
        {
            leaderDb.execute( CORE_QUERY ).close();
            executedCoreQueryLatch.countDown();
            return null;
        }, null );

        //Execute query on Replica
        threads.execute( parameter ->
        {
            replicaDb.execute( REPLICA_QUERY ).close();
            return null;
        }, null );
    }

    private Optional<Map<String,Object>> getQueryListing( String query, GraphDatabaseFacade db )
    {
        try ( Result rows = db.execute( "CALL dbms.listQueries()" ) )
        {
            while ( rows.hasNext() )
            {
                Map<String,Object> row = rows.next();
                if ( query.equals( row.get( "query" ) ) )
                {
                    return Optional.of( row );
                }
            }
        }
        return Optional.empty();
    }
}
