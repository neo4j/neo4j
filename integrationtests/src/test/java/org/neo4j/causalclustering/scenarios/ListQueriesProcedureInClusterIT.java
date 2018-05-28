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
package org.neo4j.causalclustering.scenarios;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.causalclustering.readreplica.ReadReplicaGraphDatabase;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.test.causalclustering.ClusterRule;
import org.neo4j.test.rule.VerboseTimeout;
import org.neo4j.test.rule.concurrent.ThreadingRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.test.rule.concurrent.ThreadingRule.waitingWhileIn;

public class ListQueriesProcedureInClusterIT
{
    private final ClusterRule clusterRule =
            new ClusterRule( getClass() ).withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 1 );
    private final VerboseTimeout timeout = VerboseTimeout.builder().withTimeout( 1000, SECONDS ).build();
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
        assertTrue( "setup should have created a node", matchAllResult.hasNext() );
        matchAllResult.close();

        acquireLocksAndSetupCountdownLatch( resourceLocked, listQueriesLatchOnCore, node[0] );

        resourceLocked.await();

        //When
        threads.executeAndAwait(
                executeQuery( CORE_QUERY, executedCoreQueryLatch::countDown ),/*on: */ leaderDb,
                waitingWhileIn( GraphDatabaseFacade.class, "execute" ), 5, SECONDS );

        //Then
        Optional<Map<String,Object>> coreQueryListing1 = getQueryListing( CORE_QUERY, leaderDb );
        Optional<Map<String,Object>> replicaQueryListing = getQueryListing( CORE_QUERY, replicaDb );
        Optional<Map<String,Object>> coreQueryListing2 = getQueryListing( CORE_QUERY, leaderDb );

        assertTrue( "query should be visible on core", coreQueryListing1.isPresent() );
        assertThat( coreQueryListing1.get().get( "activeLockCount" ), is( 1L ) );
        assertFalse( "query should not be visible on replica", replicaQueryListing.isPresent() );
        assertTrue(
                "query should be visible on core after it being determined not present on replicas",
                coreQueryListing2.isPresent() );

        listQueriesLatchOnCore.countDown();
        executedCoreQueryLatch.await();

        assertFalse( getQueryListing( CORE_QUERY, leaderDb ).isPresent() );
    }

    private void acquireLocksAndSetupCountdownLatch(
            CountDownLatch resourceLocked, CountDownLatch listQueriesLatch, Node node )
    {
        threads.execute( param ->
        {
            cluster.coreTx( ( leaderDb, tx ) ->
            {
                //lock node
                tx.acquireWriteLock( node );
                resourceLocked.countDown();
                try
                {
                    listQueriesLatch.await();
                }
                catch ( InterruptedException e )
                {
                    throw new AssertionError( "failure in locking node", e );
                }
            } );
            return null;
        }, null );
    }

    private ThrowingFunction<GraphDatabaseFacade,Void,RuntimeException> executeQuery( String query, Runnable then )
    {
        return db ->
        {
            db.execute( query );
            then.run();
            return null;
        };
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
