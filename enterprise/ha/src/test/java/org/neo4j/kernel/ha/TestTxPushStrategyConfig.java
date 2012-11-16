/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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

import static junit.framework.Assert.assertEquals;
import static org.neo4j.test.ha.ClusterManager.clusterOfSize;
import static org.neo4j.test.ha.ClusterManager.masterAvailable;
import static org.neo4j.test.ha.ClusterManager.masterSeesSlavesAsAvailable;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.ClusterManager;
import org.neo4j.test.ha.ClusterManager.ManagedCluster;

// TODO This needs to be fixed
@Ignore
public class TestTxPushStrategyConfig
{
    private LifeSupport life = new LifeSupport();
    private ManagedCluster cluster;
    private TargetDirectory dir;

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
        ClusterManager clusterManager = life.add( new ClusterManager( clusterOfSize( memberCount ),
                dir.directory( "dbs", true ), MapUtil.stringMap() )
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
    }

    @Test
    public void twoFixed() throws Exception
    {
        startCluster( 4, 2, "fixed" );

        for ( int i = 0; i < 5; i++ )
        {
            createTransactionOnMaster();
            assertLastTxId( 2 + i, 4 );
            assertLastTxId( 2 + i, 3 );
            assertLastTxId( 1, 2 );
        }
    }

    @Test
    public void twoRoundRobin() throws Exception
    {
        startCluster( 5, 2, "round_robin" );

        createTransactionOnMaster();
        assertLastTxId( 2, 2 );
        assertLastTxId( 2, 3 );
        assertLastTxId( 1, 4 );
        assertLastTxId( 1, 5 );

        createTransactionOnMaster();
        assertLastTxId( 2, 2 );
        assertLastTxId( 3, 3 );
        assertLastTxId( 3, 4 );
        assertLastTxId( 1, 5 );

        createTransactionOnMaster();
        assertLastTxId( 2, 2 );
        assertLastTxId( 3, 3 );
        assertLastTxId( 4, 4 );
        assertLastTxId( 4, 5 );

        createTransactionOnMaster();
        assertLastTxId( 5, 2 );
        assertLastTxId( 3, 3 );
        assertLastTxId( 4, 4 );
        assertLastTxId( 5, 5 );
    }

    @Test
    public void twoFixedFromSlaveCommit() throws Exception
    {
        startCluster( 4, 2, "fixed" );

        createTransactionOn( 2 );
        assertLastTxId( 2, 4 );
        assertLastTxId( 1, 3 );
        assertLastTxId( 2, 2 );
        assertLastTxId( 2, 1 );

        createTransactionOn( 3 );
        assertLastTxId( 3, 4 );
        assertLastTxId( 3, 3 );
        assertLastTxId( 2, 2 );
        assertLastTxId( 3, 1 );

        createTransactionOn( 4 );
        assertLastTxId( 4, 4 );
        assertLastTxId( 4, 3 );
        assertLastTxId( 2, 2 );
        assertLastTxId( 4, 1 );
    }

    @Test
    public void slavesListGetsUpdatedWhenSlaveLeavesNicely() throws Exception
    {
        startCluster( 3, 1, "fixed" );

        cluster.await( masterSeesSlavesAsAvailable( 2 ) );
        cluster.shutdown( cluster.getAnySlave() );
        cluster.await( masterSeesSlavesAsAvailable( 1 ) );
    }

    @Test
    public void slaveListIsCorrectAfterMasterSwitch() throws Exception
    {
        startCluster( 3, 1, "fixed" );

        cluster.shutdown( cluster.getMaster() );
        cluster.await( masterAvailable(), 10 );
        HighlyAvailableGraphDatabase newMaster = cluster.getMaster();
        cluster.await( masterSeesSlavesAsAvailable( 1 ) );
        createTransaction( newMaster );
        assertLastTxId( 2, 2 );
        assertLastTxId( 2, 3 );
    }

    @Test
    public void slavesListGetsUpdatedWhenSlaveRageQuits() throws Throwable
    {
        startCluster( 3, 1, "fixed" );
        cluster.fail( cluster.getAnySlave() );

        cluster.await( masterSeesSlavesAsAvailable( 1 ) );
    }

    private void assertLastTxId( long tx, int serverId )
    {
        GraphDatabaseAPI db = cluster.getMemberByServerId( serverId );  // serverId == 1 ? master : getSlave(
        // serverId );
        assertEquals( tx, db.getXaDataSourceManager().getNeoStoreDataSource().getLastCommittedTxId() );
    }

    private void createTransactionOnMaster()
    {
        createTransaction( cluster.getMaster() );
    }

    private void createTransactionOn( int serverId )
    {
        System.out.println( "Creating tx on " + serverId );
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
