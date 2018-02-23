/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.ha.lock;

import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.RuleChain;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.internal.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.test.ha.ClusterRule;

import static java.lang.Thread.sleep;
import static java.time.Duration.ofMillis;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTimeout;
import static org.junit.rules.ExpectedException.none;
import static org.junit.rules.RuleChain.outerRule;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.kernel.ha.HaSettings.tx_push_factor;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState.PENDING;
import static org.neo4j.kernel.impl.factory.GraphDatabaseFacadeFactory.Configuration.lock_manager;
import static org.neo4j.kernel.impl.ha.ClusterManager.NetworkFlag;
import static org.neo4j.kernel.impl.ha.ClusterManager.RepairKit;
import static org.neo4j.kernel.impl.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.kernel.impl.ha.ClusterManager.instanceEvicted;
import static org.neo4j.storageengine.api.EntityType.NODE;

public class ClusterLocksIT
{
    private static final long TIMEOUT_MILLIS = 120_000;

    private final ExpectedException expectedException = none();
    private final ClusterRule clusterRule = new ClusterRule();

    @Rule
    public final RuleChain rules = outerRule( expectedException ).around( clusterRule );

    private ClusterManager.ManagedCluster cluster;

    @BeforeEach
    void setUp()
    {
        cluster = clusterRule.withSharedSetting( tx_push_factor, "2" )
                .withInstanceSetting( lock_manager, i -> "community" )
                .startCluster();
    }

    private final Label testLabel = label( "testLabel" );

    @Test
    void lockCleanupOnModeSwitch() throws Throwable
    {
        assertTimeout( ofMillis( TIMEOUT_MILLIS ), () -> {
            HighlyAvailableGraphDatabase master = cluster.getMaster();
            createNodeOnMaster( testLabel, master );

            HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
            RepairKit repairKit = takeExclusiveLockAndKillSlave( testLabel, slave );

            // repair of slave and new mode switch cycle on all members
            repairKit.repair();
            cluster.await( allSeesAllAsAvailable() );

            HighlyAvailableGraphDatabase clusterMaster = cluster.getMaster();
            // now it should be possible to take exclusive lock on the same node
            takeExclusiveLockOnSameNodeAfterSwitch( testLabel, master, clusterMaster );
        } );
    }

    @Test
    void oneOrTheOtherShouldDeadlock() throws Throwable
    {
        AtomicInteger deadlockCount = new AtomicInteger();
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        Node masterA = createNodeOnMaster( testLabel, master );
        Node masterB = createNodeOnMaster( testLabel, master );

        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();

        try ( Transaction transaction = slave.beginTx() )
        {
            Node slaveA = slave.getNodeById( masterA.getId() );
            Node slaveB = slave.getNodeById( masterB.getId() );
            CountDownLatch latch = new CountDownLatch( 1 );

            transaction.acquireWriteLock( slaveB );

            Thread masterTx = new Thread( () -> {
                try ( Transaction tx = master.beginTx() )
                {
                    tx.acquireWriteLock( masterA );
                    latch.countDown();
                    tx.acquireWriteLock( masterB );
                }
                catch ( DeadlockDetectedException e )
                {
                    deadlockCount.incrementAndGet();
                }
            } );
            masterTx.start();
            latch.await();

            try
            {
                transaction.acquireWriteLock( slaveA );
            }
            catch ( DeadlockDetectedException e )
            {
                deadlockCount.incrementAndGet();
            }
            masterTx.join();
        }

        assertEquals( 1, deadlockCount.get() );
    }

    @Test
    void aPendingMemberShouldBeAbleToServeReads() throws Throwable
    {
        // given
        createNodeOnMaster( testLabel, cluster.getMaster() );
        cluster.sync();

        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
        cluster.fail( slave, NetworkFlag.values() );
        cluster.await( instanceEvicted( slave ) );

        assertEquals( PENDING, slave.getInstanceState() );

        // when
        for ( int i = 0; i < 10; i++ )
        {
            try ( Transaction tx = slave.beginTx() )
            {
                Node single = single( slave.getAllNodes() );
                Label label = single( single.getLabels() );
                assertEquals( testLabel, label );
                tx.success();
                break;
            }
            catch ( TransactionTerminatedException e )
            {
                // Race between going to pending and reading, try again in a little while
                sleep( 1_000 );
            }
        }

        // then no exceptions thrown
    }

    private void takeExclusiveLockOnSameNodeAfterSwitch( Label testLabel, HighlyAvailableGraphDatabase master,
            HighlyAvailableGraphDatabase db ) throws EntityNotFoundException
    {
        try ( Transaction transaction = db.beginTx() )
        {
            Node node = getNode( master, testLabel );
            transaction.acquireWriteLock( node );
            node.setProperty( "key", "value" );
            transaction.success();
        }
    }

    private RepairKit takeExclusiveLockAndKillSlave( Label testLabel, HighlyAvailableGraphDatabase db )
            throws EntityNotFoundException
    {
        takeExclusiveLock( testLabel, db );
        return cluster.shutdown( db );
    }

    private Transaction takeExclusiveLock( Label testLabel, HighlyAvailableGraphDatabase db )
            throws EntityNotFoundException
    {
        Transaction transaction = db.beginTx();
        Node node = getNode( db, testLabel );
        transaction.acquireWriteLock( node );
        return transaction;
    }

    private Node createNodeOnMaster( Label testLabel, HighlyAvailableGraphDatabase master )
    {
        Node node;
        try ( Transaction transaction = master.beginTx() )
        {
            node = master.createNode( testLabel );
            transaction.success();
        }
        return node;
    }

    private Node getNode( HighlyAvailableGraphDatabase db, Label testLabel ) throws EntityNotFoundException
    {
        try ( ResourceIterator<Node> nodes = db.findNodes( testLabel ) )
        {
            return nodes.stream().findFirst().orElseThrow( () -> new EntityNotFoundException( NODE, 0L ) );
        }
    }
}
