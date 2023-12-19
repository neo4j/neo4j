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
package org.neo4j.kernel.ha;

import org.junit.Rule;
import org.junit.Test;

import java.util.Map;
import java.util.concurrent.CountDownLatch;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.TransientDatabaseFailureException;
import org.neo4j.ha.TestRunConditions;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberChangeEvent;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberListener;
import org.neo4j.kernel.ha.cluster.HighAvailabilityMemberStateMachine;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.impl.ha.ClusterManager.NetworkFlag;
import org.neo4j.test.rule.LoggerRule;
import org.neo4j.test.rule.TestDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState.PENDING;
import static org.neo4j.kernel.impl.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.kernel.impl.ha.ClusterManager.instanceEvicted;
import static org.neo4j.kernel.impl.ha.ClusterManager.masterAvailable;
import static org.neo4j.kernel.impl.ha.ClusterManager.masterSeesSlavesAsAvailable;
import static org.neo4j.kernel.impl.ha.ClusterManager.memberSeesOtherMemberAsFailed;
import static org.neo4j.test.rule.DatabaseRule.tx;
import static org.neo4j.test.rule.RetryACoupleOfTimesHandler.TRANSIENT_ERRORS;
import static org.neo4j.test.rule.RetryACoupleOfTimesHandler.retryACoupleOfTimesOn;

public class ClusterPartitionIT
{
    @Rule
    public LoggerRule logger = new LoggerRule();
    @Rule
    public TestDirectory dir = TestDirectory.testDirectory();

    private final String testPropKey = "testPropKey";
    private final String testPropValue = "testPropValue";
    private long testNodeId;

    @Test
    public void isolatedMasterShouldRemoveSelfFromClusterAndBecomeReadOnly() throws Throwable
    {
        int clusterSize = 3;

        ClusterManager manager = new ClusterManager.Builder().withRootDirectory( dir.cleanDirectory( "testcluster" ) )
                .withCluster( ClusterManager.clusterOfSize( clusterSize ) )
                .build();

        try
        {
            manager.start();
            ClusterManager.ManagedCluster cluster = manager.getCluster();

            cluster.await( allSeesAllAsAvailable() );
            cluster.await( masterAvailable() );

            HighlyAvailableGraphDatabase oldMaster = cluster.getMaster();

            CountDownLatch masterTransitionLatch = new CountDownLatch( 1 );
            setupForWaitOnSwitchToDetached( oldMaster, masterTransitionLatch );

            addSomeData( oldMaster );

            ClusterManager.RepairKit fail = cluster.fail( oldMaster, NetworkFlag.values() );
            cluster.await( instanceEvicted( oldMaster ), 20 );

            masterTransitionLatch.await();

            ensureInstanceIsReadOnlyInPendingState( oldMaster );

            fail.repair();

            cluster.await( allSeesAllAsAvailable() );

            ensureInstanceIsWritable( oldMaster );
        }
        finally
        {
            manager.safeShutdown();
        }
    }

    @Test
    public void isolatedSlaveShouldRemoveSelfFromClusterAndBecomeReadOnly() throws Throwable
    {
        int clusterSize = 3;

        ClusterManager manager = new ClusterManager.Builder().withRootDirectory( dir.cleanDirectory( "testcluster" ) )
                .withCluster( ClusterManager.clusterOfSize( clusterSize ) )
                .build();

        try
        {
            manager.start();
            ClusterManager.ManagedCluster cluster = manager.getCluster();

            cluster.await( allSeesAllAsAvailable() );
            cluster.await( masterAvailable() );

            HighlyAvailableGraphDatabase slave = cluster.getAnySlave();

            CountDownLatch slaveTransitionLatch = new CountDownLatch( 1 );
            setupForWaitOnSwitchToDetached( slave, slaveTransitionLatch );

            addSomeData( slave );

            ClusterManager.RepairKit fail = cluster.fail( slave, NetworkFlag.values() );
            cluster.await( instanceEvicted( slave ), 20 );

            slaveTransitionLatch.await();

            ensureInstanceIsReadOnlyInPendingState( slave );

            fail.repair();

            cluster.await( allSeesAllAsAvailable() );

            ensureInstanceIsWritable( slave );
        }
        finally
        {
            manager.safeShutdown();
        }
    }

    @Test
    public void losingQuorumIncrementallyShouldMakeAllInstancesPendingAndReadOnly() throws Throwable
    {
        int clusterSize = 5; // we need 5 to differentiate between all other instances gone and just quorum being gone
        assumeTrue( TestRunConditions.shouldRunAtClusterSize( clusterSize ) );

        ClusterManager manager = new ClusterManager.Builder().withRootDirectory( dir.cleanDirectory( "testcluster" ) )
                .withCluster( ClusterManager.clusterOfSize( clusterSize ) )
                .withSharedConfig( config() )
                .build();

        try
        {
            manager.start();
            ClusterManager.ManagedCluster cluster = manager.getCluster();

            cluster.await( allSeesAllAsAvailable() );
            cluster.await( masterAvailable() );

            HighlyAvailableGraphDatabase master = cluster.getMaster();
            addSomeData( master );

            /*
             * we need 3 failures. We'll end up with the old master and a slave connected. They should both be in
             * PENDING state, allowing reads but not writes. Repairing just one of the removed instances should
             * result in a master being elected and all instances being read and writable.
             * The instances we remove do not need additional verification for their state. Their behaviour is already
             * known by other tests.
             */
            HighlyAvailableGraphDatabase failed1;
            ClusterManager.RepairKit rk1;
            HighlyAvailableGraphDatabase failed2;
            HighlyAvailableGraphDatabase failed3;
            HighlyAvailableGraphDatabase remainingSlave;

            failed1 = cluster.getAnySlave();
            failed2 = cluster.getAnySlave( failed1 );
            failed3 = cluster.getAnySlave( failed1, failed2 );
            remainingSlave = cluster.getAnySlave( failed1, failed2, failed3 );

            CountDownLatch masterTransitionLatch = new CountDownLatch( 1 );
            CountDownLatch slaveTransitionLatch = new CountDownLatch( 1 );

            setupForWaitOnSwitchToDetached( master, masterTransitionLatch );
            setupForWaitOnSwitchToDetached( remainingSlave, slaveTransitionLatch);

            rk1 = killIncrementally( cluster, failed1, failed2, failed3 );

            cluster.await( memberSeesOtherMemberAsFailed( remainingSlave, failed1 ) );
            cluster.await( memberSeesOtherMemberAsFailed( remainingSlave, failed2 ) );
            cluster.await( memberSeesOtherMemberAsFailed( remainingSlave, failed3 ) );

            cluster.await( memberSeesOtherMemberAsFailed( master, failed1 ) );
            cluster.await( memberSeesOtherMemberAsFailed( master, failed2 ) );
            cluster.await( memberSeesOtherMemberAsFailed( master, failed3 ) );

            masterTransitionLatch.await();
            slaveTransitionLatch.await();

            ensureInstanceIsReadOnlyInPendingState( master );
            ensureInstanceIsReadOnlyInPendingState( remainingSlave );

            rk1.repair();

            cluster.await( masterAvailable( failed2, failed3 ) );
            cluster.await( masterSeesSlavesAsAvailable( 2 ) );

            ensureInstanceIsWritable( master );
            ensureInstanceIsWritable( remainingSlave );
            ensureInstanceIsWritable( failed1 );

        }
        finally
        {
            manager.shutdown();
        }
    }

    @Test
    public void losingQuorumAbruptlyShouldMakeAllInstancesPendingAndReadOnly() throws Throwable
    {
        int clusterSize = 5; // we need 5 to differentiate between all other instances gone and just quorum being gone
        assumeTrue( TestRunConditions.shouldRunAtClusterSize( clusterSize ) );

        ClusterManager manager = new ClusterManager.Builder().withRootDirectory( dir.cleanDirectory( "testcluster" ) )
                .withCluster( ClusterManager.clusterOfSize( clusterSize ) )
                .withSharedConfig( config() )
                .build();

        try
        {
            manager.start();
            ClusterManager.ManagedCluster cluster = manager.getCluster();

            cluster.await( allSeesAllAsAvailable() );
            cluster.await( masterAvailable() );

            HighlyAvailableGraphDatabase master = cluster.getMaster();
            addSomeData( master );

            /*
             * we need 3 failures. We'll end up with the old master and a slave connected. They should both be in
             * PENDING state, allowing reads but not writes. Repairing just one of the removed instances should
             * result in a master being elected and all instances being read and writable.
             * The instances we remove do not need additional verification for their state. Their behaviour is already
             * known by other tests.
             */
            HighlyAvailableGraphDatabase failed1;
            ClusterManager.RepairKit rk1;
            HighlyAvailableGraphDatabase failed2;
            HighlyAvailableGraphDatabase failed3;
            HighlyAvailableGraphDatabase remainingSlave;

            failed1 = cluster.getAnySlave();
            failed2 = cluster.getAnySlave( failed1 );
            failed3 = cluster.getAnySlave( failed1, failed2 );
            remainingSlave = cluster.getAnySlave( failed1, failed2, failed3 );

            CountDownLatch masterTransitionLatch = new CountDownLatch( 1 );
            CountDownLatch slaveTransitionLatch = new CountDownLatch( 1 );

            setupForWaitOnSwitchToDetached( master, masterTransitionLatch );
            setupForWaitOnSwitchToDetached( remainingSlave, slaveTransitionLatch);

            rk1 = killAbruptly( cluster, failed1, failed2, failed3 );

            cluster.await( memberSeesOtherMemberAsFailed( remainingSlave, failed1 ) );
            cluster.await( memberSeesOtherMemberAsFailed( remainingSlave, failed2 ) );
            cluster.await( memberSeesOtherMemberAsFailed( remainingSlave, failed3 ) );

            cluster.await( memberSeesOtherMemberAsFailed( master, failed1 ) );
            cluster.await( memberSeesOtherMemberAsFailed( master, failed2 ) );
            cluster.await( memberSeesOtherMemberAsFailed( master, failed3 ) );

            masterTransitionLatch.await();
            slaveTransitionLatch.await();

            ensureInstanceIsReadOnlyInPendingState( master );
            ensureInstanceIsReadOnlyInPendingState( remainingSlave );

            rk1.repair();

            cluster.await( masterAvailable( failed2, failed3 ) );
            cluster.await( masterSeesSlavesAsAvailable( 2 ) );

            ensureInstanceIsWritable( master );
            ensureInstanceIsWritable( remainingSlave );
            ensureInstanceIsWritable( failed1 );
        }
        finally
        {
            manager.shutdown();
        }
    }

    private Map<String,String> config()
    {
        return stringMap(
                // so we know the initial data made it everywhere
                HaSettings.tx_push_factor.name(), "4" ,
                // increase heartbeat frequency to find the quorum in the cluster sooner to avoid timeouts
                ClusterSettings.heartbeat_interval.name(), "250ms"
        );
    }

    private ClusterManager.RepairKit killAbruptly( ClusterManager.ManagedCluster cluster,
                                                   HighlyAvailableGraphDatabase failed1,
                                                   HighlyAvailableGraphDatabase failed2,
                                                   HighlyAvailableGraphDatabase failed3 )
    {
        ClusterManager.RepairKit firstFailure = cluster.fail( failed1 );
        cluster.fail( failed2 );
        cluster.fail( failed3 );

        cluster.await( instanceEvicted( failed1 ) );
        cluster.await( instanceEvicted( failed2 ) );
        cluster.await( instanceEvicted( failed3 ) );

        return firstFailure;
    }

    private ClusterManager.RepairKit killIncrementally( ClusterManager.ManagedCluster cluster,
                                                   HighlyAvailableGraphDatabase failed1,
                                                   HighlyAvailableGraphDatabase failed2,
                                                   HighlyAvailableGraphDatabase failed3 )
    {
        ClusterManager.RepairKit firstFailure = cluster.fail( failed1 );
        cluster.await( instanceEvicted( failed1 ) );
        cluster.fail( failed2 );
        cluster.await( instanceEvicted( failed2 ) );
        cluster.fail( failed3 );
        cluster.await( instanceEvicted( failed3 ) );

        return firstFailure;
    }

    private void addSomeData( HighlyAvailableGraphDatabase instance )
    {
        try ( Transaction tx = instance.beginTx() )
        {
            Node testNode = instance.createNode();
            testNodeId = testNode.getId();
            testNode.setProperty( testPropKey, testPropValue );
            tx.success();
        }
    }

    /*
     * This method must be called on an instance that has had addSomeData() called on it.
     */
    private void ensureInstanceIsReadOnlyInPendingState( HighlyAvailableGraphDatabase instance )
    {
        assertEquals( PENDING, instance.getInstanceState() );

        tx( instance, retryACoupleOfTimesOn( TRANSIENT_ERRORS ),
                db -> assertEquals( testPropValue, instance.getNodeById( testNodeId ).getProperty( testPropKey ) ) );

        try ( Transaction ignored = instance.beginTx() )
        {
            instance.getNodeById( testNodeId ).delete();
            fail( "Should not be able to do write transactions when detached" );
        }
        catch ( TransientDatabaseFailureException | TransactionFailureException expected )
        {
            // expected
        }
    }

    private void ensureInstanceIsWritable( HighlyAvailableGraphDatabase instance )
    {
        tx( instance, retryACoupleOfTimesOn( TRANSIENT_ERRORS ),
                db -> db.createNode().setProperty( testPropKey, testPropValue ) );
    }

    private void setupForWaitOnSwitchToDetached( HighlyAvailableGraphDatabase db, final CountDownLatch latch )
    {
        db.getDependencyResolver().resolveDependency( HighAvailabilityMemberStateMachine.class )
                .addHighAvailabilityMemberListener( new HighAvailabilityMemberListener.Adapter()
                {
                    @Override
                    public void instanceDetached( HighAvailabilityMemberChangeEvent event )
                    {
                        latch.countDown();
                    }
                } );
    }
}
