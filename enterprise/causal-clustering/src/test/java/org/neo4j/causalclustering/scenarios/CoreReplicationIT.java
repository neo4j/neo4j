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


import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.RuleChain;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.neo4j.causalclustering.core.CoreGraphDatabase;
import org.neo4j.causalclustering.core.consensus.roles.Role;
import org.neo4j.causalclustering.discovery.Cluster;
import org.neo4j.causalclustering.discovery.CoreClusterMember;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.security.WriteOperationsNotAllowedException;
import org.neo4j.test.causalclustering.ClusterRule;
import org.neo4j.test.rule.VerboseTimeout;

import static org.hamcrest.CoreMatchers.containsString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.causalclustering.discovery.Cluster.dataMatchesEventually;
import static org.neo4j.function.Predicates.await;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterables.count;

public class CoreReplicationIT
{
    private final ClusterRule clusterRule =
            new ClusterRule( getClass() ).withNumberOfCoreMembers( 3 ).withNumberOfReadReplicas( 0 );
    private final VerboseTimeout timeout = VerboseTimeout.builder()
            .withTimeout( 1000, TimeUnit.SECONDS )
            .build();

    @Rule
    public RuleChain ruleChain = RuleChain.outerRule( clusterRule ).around( timeout);

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

        CoreGraphDatabase follower = cluster.getDbWithRole( Role.FOLLOWER ).database();

        // when
        try ( Transaction tx = follower.beginTx() )
        {
            follower.createNode();
            tx.success();
            fail( "Should have thrown exception" );
        }
        catch ( WriteOperationsNotAllowedException ignored )
        {
            // expected
            assertThat( ignored.getMessage(), containsString( "No write operations are allowed" ) );
        }
    }

    @Test
    public void shouldNotAllowSchemaChangesFromAFollower() throws Exception
    {
        // given
        cluster.awaitLeader();

        CoreGraphDatabase follower = cluster.getDbWithRole( Role.FOLLOWER ).database();

        // when
        try ( Transaction tx = follower.beginTx() )
        {
            follower.schema().constraintFor( Label.label( "Foo" ) ).assertPropertyIsUnique( "name" ).create();
            tx.success();
            fail( "Should have thrown exception" );
        }
        catch ( WriteOperationsNotAllowedException ignored )
        {
            // expected
            assertThat( ignored.getMessage(), containsString( "No write operations are allowed" ) );
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

        CoreGraphDatabase follower = cluster.getDbWithRole( Role.FOLLOWER ).database();

        // when
        try ( Transaction tx = follower.beginTx() )
        {
            follower.getAllNodes().iterator().next().setProperty( "name", "Mark" );
            tx.success();
            fail( "Should have thrown exception" );
        }
        catch ( WriteOperationsNotAllowedException ignored )
        {
            // expected
            assertThat( ignored.getMessage(), containsString( "No write operations are allowed" ) );
        }
    }

    private void awaitForDataToBeApplied( CoreClusterMember leader ) throws InterruptedException, TimeoutException
    {
        await( () -> countNodes(leader) > 0, 10, TimeUnit.SECONDS);
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

        cluster.removeCoreMemberWithMemberId( 0 );
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

                    cluster.removeCoreMember( cluster.getDbWithAnyRole( Role.FOLLOWER, Role.CANDIDATE ) );
                    cluster.removeCoreMember( cluster.getDbWithAnyRole( Role.FOLLOWER, Role.CANDIDATE ) );
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

    private long countNodes( CoreClusterMember member )
    {
        CoreGraphDatabase db = member.database();
        long count;
        try ( Transaction tx = db.beginTx() )
        {
            count = count( db.getAllNodes() );
            tx.success();
        }
        return count;
    }
}
