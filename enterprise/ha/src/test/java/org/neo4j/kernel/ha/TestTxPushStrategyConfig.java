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

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.SECONDS;
import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.factory.HighlyAvailableGraphDatabaseFactory;
import org.neo4j.ha.StartInstanceInAnotherJvm;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.GraphDatabaseAPI;
import org.neo4j.test.TargetDirectory;

// TODO This needs to be fixed
@Ignore
public class TestTxPushStrategyConfig
{
    private GraphDatabaseAPI master;
    private List<GraphDatabaseAPI> slaves = new ArrayList<GraphDatabaseAPI>();
    private TargetDirectory dir;
//    private Slaves slavesList;

    @Before
    public void before() throws Exception
    {
        dir = TargetDirectory.forTest( getClass() );
    }

    @After
    public void after() throws Exception
    {
        for ( GraphDatabaseService slave : slaves )
        {
            slave.shutdown();
        }
        if ( master != null )
            master.shutdown();
    }

    GraphDatabaseService startMaster( int pushFactor, String pushStrategy, int basePort )
    {
        master = (GraphDatabaseAPI) new HighlyAvailableGraphDatabaseFactory().newHighlyAvailableDatabaseBuilder( dir
                .directory( "1", true ).getAbsolutePath() )
                .setConfig( HaSettings.server_id, "1" )
                .setConfig( HaSettings.tx_push_factor, "" + pushFactor )
                .setConfig( HaSettings.cluster_discovery_enabled, "false" )
                .setConfig( HaSettings.tx_push_strategy, pushStrategy )
                .setConfig( HaSettings.cluster_server, "localhost:"+basePort )
                .newGraphDatabase();
        return master;
    }

    void addSlaves( int count, int basePort )
    {
        for ( int i = 1; i <= count; i++ )
        {
            GraphDatabaseAPI newSlave = (GraphDatabaseAPI) new HighlyAvailableGraphDatabaseFactory()
                    .newHighlyAvailableDatabaseBuilder(
                            dir.directory( "" + (1 + i), true ).getAbsolutePath() )
                    .setConfig( HaSettings.server_id, "" + (i + 1) )
                    .setConfig( HaSettings.ha_server, ":" + (6361 + i) )
                    .setConfig( HaSettings.cluster_discovery_enabled, "false" )
                    .setConfig( HaSettings.cluster_server, "localhost:" + (basePort + i) )
                    .setConfig( HaSettings.initial_hosts, "localhost:"+basePort )
                    .newGraphDatabase();
            slaves.add( newSlave );
        }

        awaitSlaveList( master, slaveCount( this.slaves.size() ) );
    }

    Process addRemoteSlave() throws Exception
    {
        int i = slaves.size()+1;
        return StartInstanceInAnotherJvm.start( dir.directory( "" + (1 + i), true ).getAbsolutePath(), MapUtil.stringMap(
                HaSettings.server_id.name(), "" + (i + 1),
                HaSettings.ha_server.name(), ":" + (6361 + i),
                HaSettings.cluster_discovery_enabled.name(), "false",
                HaSettings.cluster_server.name(), "localhost:" + (5001 + i),
                HaSettings.initial_hosts.name(), "localhost:5001" ) );
    }

    private void awaitSlaveList( GraphDatabaseAPI db, Predicate<Slaves> predicate )
    {
        Slaves slavesList = db.getDependencyResolver().resolveDependency( Slaves.class );
        long end = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis( 200 );
        while ( !predicate.accept( slavesList ) && System.currentTimeMillis() < end )
        {
            try
            {
                Thread.sleep( 100 );
            }
            catch ( InterruptedException e )
            {
                // Ignore
            }
        }      
        if ( System.currentTimeMillis() > end )
            throw new RuntimeException( "Didn't meet slave list requirement" );
    }

    private Predicate<Slaves> slaveCount( final int count )
    {
        return new Predicate<Slaves>()
        {
            @Override
            public boolean accept( Slaves slaves )
            {
                return IteratorUtil.count( slaves.getSlaves() ) == count;
            }
        };
    }

    @Test
    public void twoFixed() throws Exception
    {
        startMaster( 2, "fixed", 5001 );
        addSlaves( 3, 5001 );

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
        startMaster( 2, "round_robin",5001 );
        addSlaves( 4, 5001 );

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
        startMaster( 2, "fixed", 5001 );
        addSlaves( 3, 5001 );

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

    @Test
    public void slavesListGetsUpdatedWhenSlaveLeavesNicely() throws Exception
    {
        startMaster( 1, "fixed", 5030 );
        addSlaves( 2, 5030 );

        GraphDatabaseAPI db = slaves.remove( slaves.size() - 1 );
        db.shutdown();
        awaitSlaveList( master, slaveCount( slaves.size() ) );
    }
    
    @Test
    public void slaveListIsCorrectAfterMasterSwitch() throws Exception
    {
        startMaster( 1, "fixed", 5040 );
        addSlaves( 2, 5040 );
        
        master.shutdown();
        master = null;
        GraphDatabaseAPI newMaster = awaitNewMaster();
        awaitSlaveList( newMaster, slaveCount( 1 ) );
        createTransaction( newMaster );
        assertLastTxId( 2, 2 );
        assertLastTxId( 2, 3 );
    }

    private GraphDatabaseAPI awaitNewMaster()
    {
        long end = currentTimeMillis() + SECONDS.toMillis( 10 );
        while ( currentTimeMillis() < end )
        {
            for ( GraphDatabaseAPI db : slaves )
            {
                if ( ((HighlyAvailableGraphDatabase)db).isMaster() )
                    return db;
            }
        }
        fail( "No new master chosen" );
        return null; // fail will throw exception
    }

    @Test
    @Ignore
    public void slavesListGetsUpdatedWhenSlaveRageQuits() throws Exception
    {
        startMaster( 1, "fixed", 5050 );
        addSlaves( 1, 5050 );
        Process remoteSlave = addRemoteSlave();
        awaitSlaveList( master, slaveCount( 2 ) );

        remoteSlave.destroy();
        remoteSlave.waitFor();
        awaitSlaveList( master, slaveCount( 1 ) );
    }

    private void assertLastTxId( long tx, int serverId )
    {
        GraphDatabaseAPI db = serverId == 1 ? master : getSlave( serverId );
        assertEquals( tx, db.getXaDataSourceManager().getNeoStoreDataSource().getLastCommittedTxId() );
    }

    private GraphDatabaseAPI getSlave( int serverId )
    {
        return slaves.get( serverId - 2 );
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
