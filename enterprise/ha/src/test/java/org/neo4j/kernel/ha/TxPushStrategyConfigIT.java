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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.cluster.InstanceId;
import org.neo4j.helpers.TransactionTemplate;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.test.ha.ClusterRule;
import org.neo4j.test.rule.SuppressOutput;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.neo4j.kernel.impl.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.kernel.impl.ha.ClusterManager.clusterOfSize;
import static org.neo4j.kernel.impl.ha.ClusterManager.masterAvailable;
import static org.neo4j.kernel.impl.ha.ClusterManager.masterSeesSlavesAsAvailable;
import static org.neo4j.kernel.impl.transaction.log.TransactionIdStore.BASE_TX_ID;

public class TxPushStrategyConfigIT
{
    @Rule
    public final SuppressOutput suppressOutput = SuppressOutput.suppressAll();
    @Rule
    public final ClusterRule clusterRule = new ClusterRule();

    /**
     * These are _indexes_ of cluster members in machineIds
     */
    private static final int MASTER = 1;
    private static final int FIRST_SLAVE = 2;
    private static final int SECOND_SLAVE = 3;
    private static final int THIRD_SLAVE = 4;
    private InstanceId[] machineIds;

    private final MissedReplicasMonitor monitorListener = new MissedReplicasMonitor();

    @Test
    public void shouldPushToSlavesInDescendingOrder()
    {
        ManagedCluster cluster = startCluster( 4, 2, HaSettings.TxPushStrategy.fixed_descending );

        for ( int i = 0; i < 5; i++ )
        {
            int missed = createTransactionOnMaster( cluster );
            assertLastTransactions( cluster, lastTx( THIRD_SLAVE, BASE_TX_ID + 1 + i, missed ) );
            assertLastTransactions( cluster, lastTx( SECOND_SLAVE, BASE_TX_ID + 1 + i, missed ) );
            assertLastTransactions( cluster, lastTx( FIRST_SLAVE, BASE_TX_ID, missed ) );
        }
    }

    @Test
    public void shouldPushToSlavesInAscendingOrder()
    {
        ManagedCluster cluster = startCluster( 4, 2, HaSettings.TxPushStrategy.fixed_ascending );

        for ( int i = 0; i < 5; i++ )
        {
            int missed = createTransactionOnMaster( cluster );
            assertLastTransactions( cluster, lastTx( FIRST_SLAVE, BASE_TX_ID + 1 + i, missed ) );
            assertLastTransactions( cluster, lastTx( SECOND_SLAVE, BASE_TX_ID + 1 + i, missed ) );
            assertLastTransactions( cluster, lastTx( THIRD_SLAVE, BASE_TX_ID, missed ) );
        }
    }

    @Test
    public void twoRoundRobin()
    {
        ManagedCluster cluster = startCluster( 4, 2, HaSettings.TxPushStrategy.round_robin );

        HighlyAvailableGraphDatabase master = cluster.getMaster();
        Monitors monitors = master.getDependencyResolver().resolveDependency( Monitors.class );
        AtomicInteger totalMissedReplicas = new AtomicInteger();
        monitors.addMonitorListener( (MasterTransactionCommitProcess.Monitor) totalMissedReplicas::addAndGet );
        long txId = getLastTx( master );
        int count = 15;
        for ( int i = 0; i < count; i++ )
        {
            createTransactionOnMaster( cluster );
        }

        long min = -1;
        long max = -1;
        for ( GraphDatabaseAPI db : cluster.getAllMembers() )
        {
            long tx = getLastTx( db );
            min = min == -1 ? tx : min( min, tx );
            max = max == -1 ? tx : max( max, tx );
        }

        assertEquals( txId + count, max );
        assertTrue( "There should be members with transactions in the cluster", min != -1 && max != -1 );

        int minLaggingBehindThreshold = 1 /* this is the value without errors */ +
                totalMissedReplicas.get() /* let's consider the missed replications */;
        assertThat( "There should at most be a txId gap of 1 among the cluster members since the transaction pushing " +
                        "goes in a round robin fashion. min:" + min + ", max:" + max, (int) (max - min),
                lessThanOrEqualTo( minLaggingBehindThreshold ) );
    }

    @Test
    public void shouldPushToOneLessSlaveOnSlaveCommit()
    {
        ManagedCluster cluster = startCluster( 4, 2, HaSettings.TxPushStrategy.fixed_descending );

        int missed = 0;
        missed += createTransactionOn( cluster, new InstanceId( FIRST_SLAVE ) );
        assertLastTransactions( cluster,
                lastTx( MASTER, BASE_TX_ID + 1, missed ),
                lastTx( FIRST_SLAVE, BASE_TX_ID + 1, missed ),
                lastTx( SECOND_SLAVE, BASE_TX_ID, missed ),
                lastTx( THIRD_SLAVE, BASE_TX_ID + 1, missed ) );

        missed += createTransactionOn( cluster, new InstanceId( SECOND_SLAVE ) );
        assertLastTransactions( cluster,
                lastTx( MASTER, BASE_TX_ID + 2, missed ),
                lastTx( FIRST_SLAVE, BASE_TX_ID + 1, missed ),
                lastTx( SECOND_SLAVE, BASE_TX_ID + 2, missed ),
                lastTx( THIRD_SLAVE, BASE_TX_ID + 2, missed ) );

        missed += createTransactionOn( cluster, new InstanceId( THIRD_SLAVE ) );
        assertLastTransactions( cluster,
                lastTx( MASTER, BASE_TX_ID + 3, missed ),
                lastTx( FIRST_SLAVE, BASE_TX_ID + 1, missed ),
                lastTx( SECOND_SLAVE, BASE_TX_ID + 3, missed ),
                lastTx( THIRD_SLAVE, BASE_TX_ID + 3, missed ) );
    }

    @Test
    public void slavesListGetsUpdatedWhenSlaveLeavesNicely()
    {
        ManagedCluster cluster = startCluster( 3, 1, HaSettings.TxPushStrategy.fixed_ascending );

        cluster.shutdown( cluster.getAnySlave() );
        cluster.await( masterSeesSlavesAsAvailable( 1 ) );
    }

    @Test
    public void slaveListIsCorrectAfterMasterSwitch()
    {
        ManagedCluster cluster = startCluster( 3, 1, HaSettings.TxPushStrategy.fixed_ascending );
        cluster.shutdown( cluster.getMaster() );
        cluster.await( masterAvailable() );
        HighlyAvailableGraphDatabase newMaster = cluster.getMaster();
        cluster.await( masterSeesSlavesAsAvailable( 1 ) );
        int missed = createTransaction( cluster, newMaster );
        assertLastTransactions( cluster,
                lastTx( FIRST_SLAVE, BASE_TX_ID + 1, missed ),
                lastTx( SECOND_SLAVE, BASE_TX_ID + 1, missed ) );
    }

    @Test
    public void slavesListGetsUpdatedWhenSlaveRageQuits()
    {
        ManagedCluster cluster = startCluster( 3, 1, HaSettings.TxPushStrategy.fixed_ascending );
        cluster.fail( cluster.getAnySlave() );

        cluster.await( masterSeesSlavesAsAvailable( 1 ) );
    }

    private ManagedCluster startCluster( int memberCount, final int pushFactor, final HaSettings.TxPushStrategy pushStrategy )
    {
        ManagedCluster cluster = clusterRule.withCluster( clusterOfSize( memberCount ) )
                .withSharedSetting( HaSettings.tx_push_factor, "" + pushFactor )
                .withSharedSetting( HaSettings.tx_push_strategy, pushStrategy.name() )
                .startCluster();

        mapMachineIds( cluster );

        return cluster;
    }

    private void mapMachineIds( ManagedCluster cluster )
    {
        machineIds = new InstanceId[cluster.size()];
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        master.getDependencyResolver().resolveDependency( Monitors.class ).addMonitorListener( monitorListener );
        machineIds[0] = cluster.getServerId( master );
        List<HighlyAvailableGraphDatabase> slaves = new ArrayList<>();
        for ( HighlyAvailableGraphDatabase hadb : cluster.getAllMembers() )
        {
            if ( !hadb.isMaster() )
            {
                slaves.add( hadb );
                hadb.getDependencyResolver().resolveDependency( Monitors.class )
                        .removeMonitorListener( monitorListener );
            }
        }
        slaves.sort( Comparator.comparing( cluster::getServerId ) );
        Iterator<HighlyAvailableGraphDatabase> iter = slaves.iterator();
        for ( int i = 1; iter.hasNext(); i++ )
        {
            machineIds[i] = cluster.getServerId( iter.next() );
        }
    }

    private void assertLastTransactions( ManagedCluster cluster, LastTxMapping... transactionMappings )
    {
        StringBuilder failures = new StringBuilder();
        for ( LastTxMapping mapping : transactionMappings )
        {
            GraphDatabaseAPI db = cluster.getMemberByServerId( mapping.serverId );
            mapping.format( failures, getLastTx( db ) );
        }
        assertTrue( failures.toString(), failures.length() == 0 );
    }

    private long getLastTx( GraphDatabaseAPI db )
    {
        return db.getDependencyResolver().resolveDependency( TransactionIdStore.class )
                .getLastCommittedTransactionId();
    }

    private LastTxMapping lastTx( int serverIndex, long txId, int missed )
    {
        InstanceId serverId = machineIds[serverIndex - 1];
        return new LastTxMapping( serverId, txId, missed );
    }

    private int createTransactionOnMaster( ManagedCluster cluster )
    {
        return createTransaction( cluster, cluster.getMaster() );
    }

    private int createTransactionOn( ManagedCluster cluster, InstanceId serverId )
    {
        return createTransaction( cluster, cluster.getMemberByServerId( serverId ) );
    }

    private int createTransaction( final ManagedCluster cluster, final GraphDatabaseAPI db )
    {
        TransactionTemplate template = new TransactionTemplate()
                .with( db )
                .retries( 10 )
                .backoff( 1, TimeUnit.SECONDS )
                .monitor( new TransactionTemplate.Monitor.Adapter()
                {
                    @Override
                    public void retrying()
                    {
                        System.err.println( "Retrying..." );
                    }

                    @Override
                    public void failure( Throwable ex )
                    {
                        System.err.println( "Attempt failed with " + ex  );

                        // Assume this is because of master switch
                        // Redo the machine id mapping
                        cluster.await( allSeesAllAsAvailable() );
                        mapMachineIds( cluster );
                    }
                } );

        template.execute( transaction ->
        {
            monitorListener.clear();
            db.createNode();
        } );

        return monitorListener.missed();
    }

    private static class LastTxMapping
    {
        private final InstanceId serverId;
        private final long txId;
        private final int missed;

        LastTxMapping( InstanceId serverId, long txId, int missed )
        {
            this.serverId = serverId;
            this.txId = txId;
            this.missed = missed;
        }

        public void format( StringBuilder failures, long txId )
        {
            if ( txId < this.txId - this.missed || txId > this.txId )
            {
                if ( failures.length() > 0 )
                {
                    failures.append( ", " );
                }
                failures.append(
                        String.format( "tx id on server:%d, expected [%d] but was [%d]", serverId.toIntegerIndex(),
                                this.txId, txId ) );
            }
        }
    }

    private static class MissedReplicasMonitor implements MasterTransactionCommitProcess.Monitor
    {
        private int missed;

        @Override
        public void missedReplicas( int number )
        {
            missed = number;
        }

        int missed()
        {
            return missed;
        }

        void clear()
        {
            missed = 0;
        }
    }
}
