/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.scenarios;


import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.WriteOperationsNotAllowedException;
import org.neo4j.io.pagecache.monitoring.PageCacheCounters;
import org.neo4j.test.causalclustering.ClusterRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.causalclustering.discovery.Cluster.dataMatchesEventually;
import static org.neo4j.causalclustering.helpers.DataCreator.countNodes;
import static org.neo4j.function.Predicates.await;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.test.assertion.Assert.assertEventually;

public class CoreReplicationIT
{
    @Rule
    public final ClusterRule clusterRule =
            new ClusterRule()
                    .withNumberOfCoreMembers( 3 )
                    .withNumberOfReadReplicas( 0 )
                    .withTimeout( 1000, SECONDS );

    private Cluster cluster;

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule.startCluster();
    }

    @Test
    public void shouldReplicateTransactionsToCoreMembers() throws Exception
    {
        // when
        CoreClusterMember leader = cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        // then
        assertEquals( 1, countNodes( leader ) );
        dataMatchesEventually( leader, cluster.coreMembers() );
    }

    @Test
    public void shouldNotAllowWritesFromAFollower() throws Exception
    {
        // given
        cluster.awaitLeader();

        CoreGraphDatabase follower = cluster.getMemberWithRole( Role.FOLLOWER ).database();

        // when
        try ( Transaction tx = follower.beginTx() )
        {
            follower.createNode();
            tx.success();
            fail( "Should have thrown exception" );
        }
        catch ( WriteOperationsNotAllowedException ex )
        {
            // expected
            assertThat( ex.getMessage(), containsString( "No write operations are allowed" ) );
        }
    }

    @Test
    public void pageFaultsFromReplicationMustCountInMetrics() throws Exception
    {
        // Given initial pin counts on all members
        Function<CoreClusterMember,PageCacheCounters> getPageCacheCounters =
                ccm -> ccm.database().getDependencyResolver().resolveDependency( PageCacheCounters.class );
        List<PageCacheCounters> countersList =
                cluster.coreMembers().stream().map( getPageCacheCounters ).collect( Collectors.toList() );
        long[] initialPins = countersList.stream().mapToLong( PageCacheCounters::pins ).toArray();

        // when the leader commits a write transaction,
        cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        // then the replication should cause pins on a majority of core members to increase.
        // However, the commit returns as soon as the transaction has been replicated through the Raft log, which
        // happens before the transaction is applied on the members. Therefor we are racing with the followers
        // transaction application, so we have to spin.
        int minimumUpdatedMembersCount = countersList.size() / 2 + 1;
        assertEventually( "Expected followers to eventually increase pin counts", () ->
        {
            long[] pinsAfterCommit = countersList.stream().mapToLong( PageCacheCounters::pins ).toArray();
            int membersWithIncreasedPinCount = 0;
            for ( int i = 0; i < initialPins.length; i++ )
            {
                long before = initialPins[i];
                long after = pinsAfterCommit[i];
                if ( before < after )
                {
                    membersWithIncreasedPinCount++;
                }
            }
            return membersWithIncreasedPinCount;
        }, is( greaterThanOrEqualTo( minimumUpdatedMembersCount ) ), 10, SECONDS );
    }

    @Test
    public void shouldNotAllowSchemaChangesFromAFollower() throws Exception
    {
        // given
        cluster.awaitLeader();

        CoreGraphDatabase follower = cluster.getMemberWithRole( Role.FOLLOWER ).database();

        // when
        try ( Transaction tx = follower.beginTx() )
        {
            follower.schema().constraintFor( Label.label( "Foo" ) ).assertPropertyIsUnique( "name" ).create();
            tx.success();
            fail( "Should have thrown exception" );
        }
        catch ( WriteOperationsNotAllowedException ex )
        {
            // expected
            assertThat( ex.getMessage(), containsString( "No write operations are allowed" ) );
        }
    }

    @Test
    public void shouldNotAllowTokenCreationFromAFollowerWithNoInitialTokens() throws Exception
    {
        // given
        CoreClusterMember leader = cluster.coreTx( ( db, tx ) ->
        {
            db.createNode();
            tx.success();
        } );

        awaitForDataToBeApplied( leader );
        dataMatchesEventually( leader, cluster.coreMembers() );

        CoreGraphDatabase follower = cluster.getMemberWithRole( Role.FOLLOWER ).database();

        // when
        try ( Transaction tx = follower.beginTx() )
        {
            follower.getAllNodes().iterator().next().setProperty( "name", "Mark" );
            tx.success();
            fail( "Should have thrown exception" );
        }
        catch ( WriteOperationsNotAllowedException ex )
        {
            assertThat( ex.getMessage(), containsString( "No write operations are allowed" ) );
        }
    }

    private void awaitForDataToBeApplied( CoreClusterMember leader ) throws TimeoutException
    {
        await( () -> countNodes(leader) > 0, 10, SECONDS);
    }

    @Test
    public void shouldReplicateTransactionToCoreMemberAddedAfterInitialStartUp() throws Exception
    {
        // given
        cluster.getCoreMemberById( 0 ).shutdown();

        cluster.addCoreMemberWithId( 3 ).start();
        cluster.getCoreMemberById( 0 ).start();

        cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode();
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        // when
        cluster.addCoreMemberWithId( 4 ).start();
        CoreClusterMember last = cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode();
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        // then
        assertEquals( 2, countNodes( last ) );
        dataMatchesEventually( last, cluster.coreMembers() );
    }

    @Test
    public void shouldReplicateTransactionAfterLeaderWasRemovedFromCluster() throws Exception
    {
        // given
        cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode();
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        // when
        cluster.removeCoreMember( cluster.awaitLeader() );
        cluster.awaitLeader( 1, TimeUnit.MINUTES ); // <- let's give a bit more time for the leader to show up
        CoreClusterMember last = cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode();
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        // then
        assertEquals( 2, countNodes( last ) );
        dataMatchesEventually( last, cluster.coreMembers() );
    }

    @Test
    public void shouldReplicateToCoreMembersAddedAfterInitialTransactions() throws Exception
    {
        // when
        CoreClusterMember last = null;
        for ( int i = 0; i < 15; i++ )
        {
            last = cluster.coreTx( ( db, tx ) ->
            {
                Node node = db.createNode();
                node.setProperty( "foobar", "baz_bat" );
                tx.success();
            } );
        }

        cluster.addCoreMemberWithId( 3 ).start();
        cluster.addCoreMemberWithId( 4 ).start();

        // then
        assertEquals( 15, countNodes( last ) );
        dataMatchesEventually( last, cluster.coreMembers() );
    }

    @Test
    public void shouldReplicateTransactionsToReplacementCoreMembers() throws Exception
    {
        // when
        cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        cluster.removeCoreMemberWithServerId( 0 );
        CoreClusterMember replacement = cluster.addCoreMemberWithId( 0 );
        replacement.start();

        CoreClusterMember leader = cluster.coreTx( ( db, tx ) ->
        {
            db.schema().indexFor( label( "boo" ) ).on( "foobar" ).create();
            tx.success();
        } );

        // then
        assertEquals( 1, countNodes( leader ) );
        dataMatchesEventually( leader, cluster.coreMembers() );
    }

    @Test
    public void shouldBeAbleToShutdownWhenTheLeaderIsTryingToReplicateTransaction() throws Exception
    {
        // given
        cluster.coreTx( ( db, tx ) ->
        {
            Node node = db.createNode( label( "boo" ) );
            node.setProperty( "foobar", "baz_bat" );
            tx.success();
        } );

        CountDownLatch latch = new CountDownLatch( 1 );

        // when
        Thread thread = new Thread( () ->
        {
            try
            {
                cluster.coreTx( ( db, tx ) ->
                {
                    db.createNode();
                    tx.success();

                    cluster.removeCoreMember( cluster.getMemberWithAnyRole( Role.FOLLOWER, Role.CANDIDATE ) );
                    cluster.removeCoreMember( cluster.getMemberWithAnyRole( Role.FOLLOWER, Role.CANDIDATE ) );
                    latch.countDown();
                } );
                fail( "Should have thrown" );
            }
            catch ( Exception ignored )
            {
                // expected
            }
        } );

        thread.start();

        latch.await();

        // then the cluster can shutdown...
        cluster.shutdown();
        // ... and the thread running the tx does not get stuck
        thread.join( TimeUnit.MINUTES.toMillis( 1 ) );
    }
}
