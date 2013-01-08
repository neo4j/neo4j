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

import static junit.framework.Assert.assertEquals;

import java.util.ArrayList;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EnterpriseGraphDatabaseFactory;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.test.TargetDirectory;
import org.neo4j.test.ha.LocalhostZooKeeperCluster;

public class TestTxPushStrategyConfig
{
    private LocalhostZooKeeperCluster zoo;
    private GraphDatabaseAPI master;
    private List<GraphDatabaseAPI> slaves = new ArrayList<GraphDatabaseAPI>();
    private TargetDirectory dir;
    
    @Before
    public void before() throws Exception
    {
        dir = TargetDirectory.forTest( getClass() );
        zoo = LocalhostZooKeeperCluster.singleton().clearDataAndVerifyConnection();
    }

    @After
    public void after() throws Exception
    {
        for ( GraphDatabaseService slave : slaves )
            slave.shutdown();
        master.shutdown();
    }
    
    GraphDatabaseService startMaster( int pushFactor, String pushStrategy )
    {
        master = (GraphDatabaseAPI) new EnterpriseGraphDatabaseFactory().newHighlyAvailableDatabaseBuilder( dir.directory( "1", true ).getAbsolutePath() )
                .setConfig( HaSettings.coordinators, zoo.getConnectionString() )
                .setConfig( HaSettings.server_id, "1" )
                .setConfig( HaSettings.server, "localhost:6361" )
                .setConfig( HaSettings.tx_push_factor, "" + pushFactor )
                .setConfig( HaSettings.tx_push_strategy, pushStrategy )
                .newGraphDatabase();
        return master;
    }
    
    void addSlaves( int count )
    {
        for ( int i = 1; i <= count; i++ )
            slaves.add( (GraphDatabaseAPI) new EnterpriseGraphDatabaseFactory().newHighlyAvailableDatabaseBuilder( dir.directory( "" + (1+i), true ).getAbsolutePath() )
                    .setConfig( HaSettings.coordinators, zoo.getConnectionString() )
                    .setConfig( HaSettings.server_id, "" + (i+1) )
                    .setConfig( HaSettings.server, "localhost:" + (6361+i) )
                    .newGraphDatabase() );
    }
    
    @Test
    public void twoFixed() throws Exception
    {
        startMaster( 2, "fixed" );
        addSlaves( 3 );
        
        for ( int i = 0; i < 5; i++ )
        {
            createTransactionOnMaster();
            assertLastTxId( 2+i, 4 );
            assertLastTxId( 2+i, 3 );
            assertLastTxId( 1, 2 );
        }
    }
    
    @Test
    public void twoRoundRobin() throws Exception
    {
        startMaster( 2, "round_robin" );
        addSlaves( 4 );
        
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
        startMaster( 2, "fixed" );
        addSlaves( 3 );
        
        createTransactionOnSlave( 2 );
        assertLastTxId( 2, 4 );
        assertLastTxId( 1, 3 );
        assertLastTxId( 2, 2 );
        assertLastTxId( 2, 1 );

        createTransactionOnSlave( 3 );
        assertLastTxId( 3, 4 );
        assertLastTxId( 3, 3 );
        assertLastTxId( 2, 2 );
        assertLastTxId( 3, 1 );

        createTransactionOnSlave( 4 );
        assertLastTxId( 4, 4 );
        assertLastTxId( 4, 3 );
        assertLastTxId( 2, 2 );
        assertLastTxId( 4, 1 );
    }
    
    private void assertLastTxId( long tx, int serverId )
    {
        GraphDatabaseAPI db = serverId == 1 ? master : getSlave( serverId );
        assertEquals( tx, db.getXaDataSourceManager().getNeoStoreDataSource().getLastCommittedTxId() );
    }

    private GraphDatabaseAPI getSlave( int serverId )
    {
        return slaves.get( serverId-2 );
    }

    private void createTransactionOnMaster()
    {
        createTransaction( master );
    }
    
    private void createTransactionOnSlave( int serverId )
    {
        System.out.println( "Creating tx on " + serverId );
        createTransaction( getSlave( serverId ) );
    }

    private void createTransaction( GraphDatabaseAPI db )
    {
        Transaction tx = db.beginTx();
        try
        {
            db.createNode();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }
}
