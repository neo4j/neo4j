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
package org.neo4j.kernel.ha;

import org.junit.Rule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.neo4j.cluster.InstanceId;
import org.neo4j.function.Consumer;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.TransactionTemplate;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.ha.ClusterManager.ManagedCluster;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.test.SuppressOutput;
import org.neo4j.test.ha.ClusterRule;

import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import static java.lang.Math.max;
import static java.lang.Math.min;

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
    public final ClusterRule clusterRule = new ClusterRule( getClass() );

    /**
     * These are _indexes_ of cluster members in machineIds
     */
    private static final int MASTER = 1;
    private static final int FIRST_SLAVE = 2;
    private static final int SECOND_SLAVE = 3;
    private static final int THIRD_SLAVE = 4;
    private InstanceId[] machineIds;

    @Test
    public void shouldPushToSlavesInDescendingOrder() throws Exception
    {
        ManagedCluster cluster = startCluster( 4, 2, HaSettings.TxPushStrategy.fixed_descending );

        for ( int i = 0; i < 5; i++ )
        {
            createTransactionOnMaster( cluster );
            assertLastTransactions( cluster, lastTx( THIRD_SLAVE, BASE_TX_ID + 1 + i ) );
            assertLastTransactions( cluster, lastTx( SECOND_SLAVE, BASE_TX_ID + 1 + i ) );
            assertLastTransactions( cluster, lastTx( FIRST_SLAVE, BASE_TX_ID ) );
        }
    }


    @Test
    public void shouldPushToSlavesInAscendingOrder() throws Exception
    {
        ManagedCluster cluster = startCluster( 4, 2, HaSettings.TxPushStrategy.fixed_ascending );

        for ( int i = 0; i < 5; i++ )
        {
            createTransactionOnMaster( cluster );
            assertLastTransactions( cluster, lastTx( FIRST_SLAVE, BASE_TX_ID + 1 + i ) );
            assertLastTransactions( cluster, lastTx( SECOND_SLAVE, BASE_TX_ID + 1 + i ) );
            assertLastTransactions( cluster, lastTx( THIRD_SLAVE, BASE_TX_ID ) );
        }
    }

    @Test
    public void twoRoundRobin() throws Exception
    {
        ManagedCluster cluster = startCluster( 4, 2, HaSettings.TxPushStrategy.round_robin );

        long txId = getLastTx( cluster.getMaster() );
        int count = 15;
        for ( int i = 0; i < count; i++ )
        {
            createTransactionOnMaster( cluster );
        }

        long min = -1, max = -1;
        for ( GraphDatabaseAPI db : cluster.getAllMembers() )
        {
            long tx = getLastTx( db );
            min = min == -1 ? tx : min( min, tx );
            max = max == -1 ? tx : max( max, tx );
        }

        assertEquals( txId + count, max );
        assertTrue( "There should be members with transactions in the cluster", min != -1 );
        assertTrue( "There should be members with transactions in the cluster", max != -1 );
        assertThat( "There should at most be a txId gap of 1 among the cluster members since the transaction pushing " +
                "goes in a round robin fashion. min:" + min + ", max:" + max,
                (int) (max - min), lessThanOrEqualTo( 1 ) );
    }

    @Test
    public void shouldPushToOneLessSlaveOnSlaveCommit() throws Exception
    {
        ManagedCluster cluster = startCluster( 4, 2, HaSettings.TxPushStrategy.fixed_descending );

        createTransactionOn( cluster, new InstanceId( FIRST_SLAVE ) );
        assertLastTransactions( cluster,
                lastTx( MASTER, BASE_TX_ID + 1 ),
                lastTx( FIRST_SLAVE, BASE_TX_ID + 1 ),
                lastTx( SECOND_SLAVE, BASE_TX_ID ),
                lastTx( THIRD_SLAVE, BASE_TX_ID + 1 ) );

        createTransactionOn( cluster, new InstanceId( SECOND_SLAVE ) );
        assertLastTransactions( cluster,
                lastTx( MASTER, BASE_TX_ID + 2 ),
                lastTx( FIRST_SLAVE, BASE_TX_ID + 1 ),
                lastTx( SECOND_SLAVE, BASE_TX_ID + 2 ),
                lastTx( THIRD_SLAVE, BASE_TX_ID + 2 ) );

        createTransactionOn( cluster, new InstanceId( THIRD_SLAVE ) );
        assertLastTransactions( cluster,
                lastTx( MASTER, BASE_TX_ID + 3 ),
                lastTx( FIRST_SLAVE, BASE_TX_ID + 1 ),
                lastTx( SECOND_SLAVE, BASE_TX_ID + 3 ),
                lastTx( THIRD_SLAVE, BASE_TX_ID + 3 ) );
    }

    @Test
    public void slavesListGetsUpdatedWhenSlaveLeavesNicely() throws Exception
    {
        ManagedCluster cluster = startCluster( 3, 1, HaSettings.TxPushStrategy.fixed_ascending );

        cluster.shutdown( cluster.getAnySlave() );
        cluster.await( masterSeesSlavesAsAvailable( 1 ) );
    }

    @Test
    public void slaveListIsCorrectAfterMasterSwitch() throws Exception
    {
        ManagedCluster cluster = startCluster( 3, 1, HaSettings.TxPushStrategy.fixed_ascending );
        cluster.shutdown( cluster.getMaster() );
        cluster.await( masterAvailable() );
        HighlyAvailableGraphDatabase newMaster = cluster.getMaster();
        cluster.await( masterSeesSlavesAsAvailable( 1 ) );
        createTransaction( cluster, newMaster );
        assertLastTransactions( cluster,
                lastTx( FIRST_SLAVE, BASE_TX_ID + 1 ),
                lastTx( SECOND_SLAVE, BASE_TX_ID + 1 ) );
    }

    @Test
    public void slavesListGetsUpdatedWhenSlaveRageQuits() throws Throwable
    {
        ManagedCluster cluster = startCluster( 3, 1, HaSettings.TxPushStrategy.fixed_ascending );
        cluster.fail( cluster.getAnySlave() );

        cluster.await( masterSeesSlavesAsAvailable( 1 ) );
    }

    private ManagedCluster startCluster( int memberCount, final int pushFactor, final HaSettings.TxPushStrategy pushStrategy )
            throws Exception
    {
        ManagedCluster cluster = clusterRule.withProvider( clusterOfSize( memberCount ) )
                .withSharedSetting( HaSettings.tx_push_factor, "" + pushFactor )
                .withSharedSetting( HaSettings.tx_push_strategy, pushStrategy.name() )
                .startCluster();

        mapMachineIds( cluster );

        return cluster;
    }

    private void mapMachineIds(final  ManagedCluster cluster )
    {
        machineIds = new InstanceId[cluster.size()];
        machineIds[0] = cluster.getServerId( cluster.getMaster() );
        List<HighlyAvailableGraphDatabase> slaves = new ArrayList<>();
        for ( HighlyAvailableGraphDatabase hadb : cluster.getAllMembers() )
        {
            if ( !hadb.isMaster() )
            {
                slaves.add( hadb );
            }
        }
        Collections.sort( slaves, new Comparator<HighlyAvailableGraphDatabase>()
        {
            @Override
            public int compare( HighlyAvailableGraphDatabase o1, HighlyAvailableGraphDatabase o2 )
            {
                return cluster.getServerId( o1 ) .compareTo(  cluster.getServerId( o2 ) );
            }
        } );
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

    private LastTxMapping lastTx( int serverIndex, long txId )
    {
        InstanceId serverId = machineIds[serverIndex - 1];
        return new LastTxMapping( serverId, txId );
    }

    private void createTransactionOnMaster( ManagedCluster cluster )
    {
        createTransaction( cluster, cluster.getMaster() );
    }

    private void createTransactionOn( ManagedCluster cluster, InstanceId serverId )
    {
        createTransaction( cluster, cluster.getMemberByServerId( serverId ) );
    }

    private void createTransaction( final ManagedCluster cluster, final GraphDatabaseAPI db )
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

        template.execute( new Consumer<Transaction>()
        {
            @Override
            public void accept( Transaction transaction )
            {
                db.createNode();
            }
        } );
    }

    private static class LastTxMapping
    {
        private final InstanceId serverId;
        private final long txId;

        public LastTxMapping( InstanceId serverId, long txId )
        {
            this.serverId = serverId;
            this.txId = txId;
        }

        public void format( StringBuilder failures, long txId )
        {
            if ( txId != this.txId )
            {
                if ( failures.length() > 0 )
                    failures.append( ", " );
                failures.append( String.format( "tx id on server:%d, expected [%d] but was [%d]",
                        serverId.toIntegerIndex(), this.txId, txId ) );
            }
        }
    }

}
