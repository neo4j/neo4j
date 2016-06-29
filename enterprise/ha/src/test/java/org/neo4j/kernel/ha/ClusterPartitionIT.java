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
package org.neo4j.kernel.ha;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.InstanceId;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransientDatabaseFailureException;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.kernel.impl.ha.ClusterManager.NetworkFlag;
import org.neo4j.test.LoggerRule;
import org.neo4j.test.TargetDirectory;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.ha.cluster.HighAvailabilityMemberState.PENDING;
import static org.neo4j.kernel.impl.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.kernel.impl.ha.ClusterManager.masterAvailable;
import static org.neo4j.kernel.impl.ha.ClusterManager.masterSeesSlavesAsAvailable;
import static org.neo4j.kernel.impl.ha.ClusterManager.memberSeesOtherMemberAsFailed;

import java.util.function.Predicate;

public class ClusterPartitionIT
{
    @Rule
    public LoggerRule logger = new LoggerRule();
    @Rule
    public TargetDirectory.TestDirectory dir = TargetDirectory.testDirForTest( getClass() );

    private final String testPropKey = "testPropKey";
    private final String testPropValue = "testPropValue";
    private long testNodeId;

    @Test
    public void isolatedMasterShouldRemoveSelfFromClusterAndBecomeReadOnly() throws Throwable
    {
        int clusterSize = 3;

        ClusterManager manager = new ClusterManager.Builder().withRootDirectory( dir.cleanDirectory( "testcluster" ) )
                .withCluster( ClusterManager.clusterOfSize( clusterSize ) )
                .withSharedConfig( stringMap(
                        ClusterSettings.heartbeat_interval.name(), "1" ) )
                .build();

        try
        {
            manager.start();
            ClusterManager.ManagedCluster cluster = manager.getCluster();

            cluster.await( allSeesAllAsAvailable() );
            cluster.await( masterAvailable() );

            HighlyAvailableGraphDatabase oldMaster = cluster.getMaster();

            addSomeData( oldMaster );

            ClusterManager.RepairKit fail = cluster.fail( oldMaster, NetworkFlag.values() );
            cluster.await( instanceEvicted( oldMaster ), 20 );

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
                .withSharedConfig( stringMap(
                        ClusterSettings.heartbeat_interval.name(), "1" ) )
                .build();

        try
        {
            manager.start();
            ClusterManager.ManagedCluster cluster = manager.getCluster();

            cluster.await( allSeesAllAsAvailable() );
            cluster.await( masterAvailable() );

            HighlyAvailableGraphDatabase slave = cluster.getAnySlave();

            addSomeData( slave );

            ClusterManager.RepairKit fail = cluster.fail( slave, NetworkFlag.values() );
            cluster.await( instanceEvicted( slave ), 20 );

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

        ClusterManager manager = new ClusterManager.Builder().withRootDirectory( dir.cleanDirectory( "testcluster" ) )
                .withCluster( ClusterManager.clusterOfSize( clusterSize ) )
                .withSharedConfig( stringMap(
                        ClusterSettings.heartbeat_interval.name(), "1",
                        ClusterSettings.heartbeat_timeout.name(), "3",
                        HaSettings.tx_push_factor.name(), "4" ) ) // so we know the initial data made it everywhere
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

            rk1 = killIncrementally( cluster, failed1, failed2, failed3 );

            cluster.await( memberSeesOtherMemberAsFailed( remainingSlave, failed1 ) );
            cluster.await( memberSeesOtherMemberAsFailed( remainingSlave, failed2 ) );
            cluster.await( memberSeesOtherMemberAsFailed( remainingSlave, failed3 ) );

            cluster.await( memberSeesOtherMemberAsFailed( master, failed1 ) );
            cluster.await( memberSeesOtherMemberAsFailed( master, failed2 ) );
            cluster.await( memberSeesOtherMemberAsFailed( master, failed3 ) );

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

        ClusterManager manager = new ClusterManager.Builder().withRootDirectory( dir.cleanDirectory( "testcluster" ) )
                .withCluster( ClusterManager.clusterOfSize( clusterSize ) )
                .withSharedConfig( stringMap(
                        ClusterSettings.heartbeat_interval.name(), "1",
                        ClusterSettings.heartbeat_timeout.name(), "3",
                        HaSettings.tx_push_factor.name(), "4" ) ) // so we know the initial data made it everywhere
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

            rk1 = killAbruptly( cluster, failed1, failed2, failed3 );

            cluster.await( memberSeesOtherMemberAsFailed( remainingSlave, failed1 ) );
            cluster.await( memberSeesOtherMemberAsFailed( remainingSlave, failed2 ) );
            cluster.await( memberSeesOtherMemberAsFailed( remainingSlave, failed3 ) );

            cluster.await( memberSeesOtherMemberAsFailed( master, failed1 ) );
            cluster.await( memberSeesOtherMemberAsFailed( master, failed2 ) );
            cluster.await( memberSeesOtherMemberAsFailed( master, failed3 ) );

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

    private ClusterManager.RepairKit killAbruptly( ClusterManager.ManagedCluster cluster,
                                                   HighlyAvailableGraphDatabase failed1,
                                                   HighlyAvailableGraphDatabase failed2,
                                                   HighlyAvailableGraphDatabase failed3 ) throws Throwable
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
                                                   HighlyAvailableGraphDatabase failed3 ) throws Throwable
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

        try ( Transaction tx = instance.beginTx() )
        {
            assertEquals( testPropValue, instance.getNodeById( testNodeId ).getProperty( testPropKey ) );
            tx.success();
        }

        try ( Transaction ignored = instance.beginTx() )
        {
            instance.getNodeById( testNodeId ).delete();
            fail( "Should not be able to do write transactions when detached" );
        }
        catch ( TransientDatabaseFailureException e )
        {
            // expected
        }
    }

    private void ensureInstanceIsWritable( HighlyAvailableGraphDatabase instance )
    {
        try ( Transaction tx = instance.beginTx() )
        {
            instance.createNode().setProperty( testPropKey, testPropValue );
            tx.success();
        }
    }

    private Predicate<ClusterManager.ManagedCluster> instanceEvicted( final HighlyAvailableGraphDatabase instance )
    {
        return managedCluster -> {
            InstanceId instanceId = managedCluster.getServerId( instance );

            Iterable<HighlyAvailableGraphDatabase> members = managedCluster.getAllMembers();
            for ( HighlyAvailableGraphDatabase member : members )
            {
                if ( instanceId.equals( managedCluster.getServerId( member ) ) )
                {
                    if ( member.role().equals( "UNKNOWN" ) )
                    {
                        return true;
                    }
                }
            }
            return false;

        };
    }
}
