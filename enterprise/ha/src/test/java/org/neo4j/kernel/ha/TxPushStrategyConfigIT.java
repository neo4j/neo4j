/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.impl.transaction.XaDataSourceManager;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.ClusterManager;
import org.neo4j.test.ha.ClusterManager.ManagedCluster;

import static org.junit.Assert.assertTrue;

import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME;
import static org.neo4j.test.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.test.ha.ClusterManager.clusterOfSize;
import static org.neo4j.test.ha.ClusterManager.masterAvailable;
import static org.neo4j.test.ha.ClusterManager.masterSeesSlavesAsAvailable;

public class TxPushStrategyConfigIT
{
    @Test
    public void twoFixed() throws Exception
    {
        startCluster( 4, 2, "fixed" );
        
        for ( int i = 0; i < 5; i++ )
        {
            createTransactionOnMaster();
            assertLastTransactions( lastTx( 4, 2 + i ) );
            assertLastTransactions( lastTx( 3, 2 + i ) );
            assertLastTransactions( lastTx( 2, 1 ) );
        }
    }

    @Test
    public void twoRoundRobin() throws Exception
    {
        startCluster( 5, 2, "round_robin" );

        createTransactionOnMaster();
        assertLastTransactions( lastTx( 2, 2 ), lastTx( 3, 2 ), lastTx( 4, 1 ), lastTx( 5, 1 ) );

        createTransactionOnMaster();
        assertLastTransactions( lastTx( 2, 2 ), lastTx( 3, 3 ), lastTx( 4, 3 ), lastTx( 5, 1 ) );

        createTransactionOnMaster();
        assertLastTransactions( lastTx( 2, 2 ), lastTx( 3, 3 ), lastTx( 4, 4 ), lastTx( 5, 4 ) );

        createTransactionOnMaster();
        assertLastTransactions( lastTx( 2, 5 ), lastTx( 3, 3 ), lastTx( 4, 4 ), lastTx( 5, 5 ) );
    }

    @Test
    public void twoFixedFromSlaveCommit() throws Exception
    {
        startCluster( 4, 2, "fixed" );

        createTransactionOn( 2 );
        assertLastTransactions( lastTx( 4, 2 ), lastTx( 3, 1 ), lastTx( 2, 2 ), lastTx( 1, 2 ) );

        createTransactionOn( 3 );
        assertLastTransactions( lastTx( 4, 3 ), lastTx( 3, 3 ), lastTx( 2, 2 ), lastTx( 1, 3 ) );

        createTransactionOn( 4 );
        assertLastTransactions( lastTx( 4, 4 ), lastTx( 3, 4 ), lastTx( 2, 2 ), lastTx( 1, 4 ) );
    }

    @Test
    public void slavesListGetsUpdatedWhenSlaveLeavesNicely() throws Exception
    {
        startCluster( 3, 1, "fixed" );

        cluster.shutdown( cluster.getAnySlave() );
        cluster.await( masterSeesSlavesAsAvailable( 1 ) );
    }

    @Test
    public void slaveListIsCorrectAfterMasterSwitch() throws Exception
    {
        startCluster( 3, 1, "fixed" );
        cluster.shutdown( cluster.getMaster() );
        cluster.await( masterAvailable() );
        HighlyAvailableGraphDatabase newMaster = cluster.getMaster();
        cluster.await( masterSeesSlavesAsAvailable( 1 ) );
        createTransaction( newMaster );
        assertLastTransactions( lastTx( 2, 2 ), lastTx( 3, 2 ) );
    }

    @Test
    public void slavesListGetsUpdatedWhenSlaveRageQuits() throws Throwable
    {
        startCluster( 3, 1, "fixed" );
        cluster.fail( cluster.getAnySlave() );

        cluster.await( masterSeesSlavesAsAvailable( 1 ) );
    }

    private final LifeSupport life = new LifeSupport();
    private ManagedCluster cluster;
    private TargetDirectory dir;

    @Rule
    public TestName name = new TestName();
    private int[] machineIds;

    @Before
    public void before() throws Exception
    {
        dir = TargetDirectory.forTest( getClass() );
    }

    @After
    public void after() throws Exception
    {
        life.shutdown();
    }

    private void startCluster( int memberCount, final int pushFactor, final String pushStrategy )
    {
        ClusterManager clusterManager = life.add( new ClusterManager( clusterOfSize( memberCount ), dir.directory(
                name.getMethodName(), true ), stringMap() )
        {
            @Override
            protected void config( GraphDatabaseBuilder builder, String clusterName, int serverId )
            {
                builder.setConfig( HaSettings.tx_push_factor, "" + pushFactor );
                builder.setConfig( HaSettings.tx_push_strategy, pushStrategy );
            }
        } );
        life.start();
        cluster = clusterManager.getDefaultCluster();
        cluster.await( allSeesAllAsAvailable() );

        mapMachineIds();
    }

    private void mapMachineIds()
    {
        machineIds = new int[cluster.size()];
        machineIds[0] = cluster.getServerId( cluster.getMaster() );
        List<HighlyAvailableGraphDatabase> slaves = new ArrayList<HighlyAvailableGraphDatabase>();
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
                return cluster.getServerId( o1 ) - cluster.getServerId( o2 );
            }
        } );
        Iterator<HighlyAvailableGraphDatabase> iter = slaves.iterator();
        for ( int i = 1; iter.hasNext(); i++ )
        {
            machineIds[i] = cluster.getServerId( iter.next() );
        }
    }

    private void assertLastTransactions( LastTxMapping... transactionMappings )
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
        return db.getDependencyResolver().resolveDependency( XaDataSourceManager.class )
                .getXaDataSource( DEFAULT_DATA_SOURCE_NAME ).getLastCommittedTxId();
    }

    private LastTxMapping lastTx( int serverIndex, long txId )
    {
        int serverId = machineIds[serverIndex - 1];
        return new LastTxMapping( serverId, txId );
    }

    private static class LastTxMapping
    {
        private final int serverId;
        private final long txId;

        public LastTxMapping( int serverId, long txId )
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
                        serverId, this.txId, txId ) );
            }
        }
    }

    private void createTransactionOnMaster()
    {
        createTransaction( cluster.getMaster() );
    }

    private void createTransactionOn( int serverId )
    {
        createTransaction( cluster.getMemberByServerId( serverId ) );
    }

    private void createTransaction( GraphDatabaseAPI db )
    {
        Transaction tx = db.beginTx();
        try
        {
            db.createNode();
            tx.success();
        }
        catch ( RuntimeException e )
        {
            e.printStackTrace();
            throw e;
        }
        finally
        {
            tx.finish();
        }
    }
}
