/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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
package slavetest;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.kernel.impl.transaction.TestRecovery.countMentionsInMessagesLog;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.Collection;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;
import org.neo4j.com.Client;
import org.neo4j.com.Protocol;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.AbstractBroker;
import org.neo4j.kernel.ha.Broker;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.ha.MasterClient;
import org.neo4j.kernel.ha.MasterImpl;
import org.neo4j.kernel.ha.zookeeper.Machine;
import org.neo4j.kernel.impl.util.FileUtils;

public class SingleJvmWithNettyTest extends SingleJvmTest
{
    @Test
    public void assertThatNettyIsUsed() throws Exception
    {
        initializeDbs( 1 );
        assertTrue(
                "Slave Broker is not a client",
                ( (HighlyAvailableGraphDatabase) getSlave( 0 ) ).getBroker().getMaster().first() instanceof MasterClient );
    }

    @Override
    protected Broker makeSlaveBroker( MasterImpl master, int masterId, int id, GraphDatabaseService graphDb )
    {
        final Machine masterMachine = new Machine( masterId, -1, 1, -1,
                "localhost:" + Protocol.PORT );
        final Master client = new MasterClient( masterMachine.getServer().first(), masterMachine.getServer().other(), graphDb,
                Client.DEFAULT_MAX_NUMBER_OF_CONCURRENT_CHANNELS_PER_CLIENT);
        return new AbstractBroker( id, graphDb )
        {
            public boolean iAmMaster()
            {
                return false;
            }

            public Pair<Master, Machine> getMasterReally()
            {
                return getMaster();
            }

            public Pair<Master, Machine> getMaster()
            {
                return Pair.of( client, masterMachine );
            }

            public Object instantiateMasterServer( GraphDatabaseService graphDb )
            {
                throw new UnsupportedOperationException(
                        "cannot instantiate master server on slave" );
            }
        };
    }

    @Test
    public void makeSureLogMessagesIsWrittenEvenAfterInternalRestart() throws Exception
    {
        initializeDbs( 1 );
        final CountDownLatch latch1 = new CountDownLatch( 1 );
        final GraphDatabaseService slave = getSlave( 0 );
        Thread t1 = new Thread()
        {
            @Override
            public void run()
            {
                try
                {
                    Transaction tx = slave.beginTx();
                    slave.createNode();
                    latch1.await();
                    tx.success();
                    tx.finish();
                }
                catch ( InterruptedException e )
                {
                    throw new RuntimeException( e );
                }
            }
        };
        t1.start();

        Thread t2 = new Thread()
        {
            @Override
            public void run()
            {
                Transaction tx = slave.beginTx();
                slave.createNode();
                latch1.countDown();
                tx.success();
                tx.finish();
            }
        };
        t2.start();
        
        t1.join();
        t2.join();
        
        assertEquals( 2, countOccurences( "Opened a new channel", new File( dbPath( 1 ), "messages.log" ) ) );
    }

    private int countOccurences( String string, File file ) throws Exception
    {
        BufferedReader reader = new BufferedReader( new FileReader( file ) );
        String line = null;
        int counter = 0;
        while ( (line = reader.readLine()) != null )
        {
            System.out.println( line );
            if ( line.contains( string ) ) counter++;
        }
        reader.close();
        return counter;
    }

    @Test
    public void testMixingEntitiesFromWrongDbs() throws Exception
    {
        initializeDbs( 1 );
        GraphDatabaseService haDb1 = getSlave( 0 );
        GraphDatabaseService mDb = getMaster().getGraphDb();

        Transaction tx = mDb.beginTx();
        Node masterNode;
        try
        {
            masterNode = mDb.createNode();
            mDb.getReferenceNode().createRelationshipTo( masterNode, CommonJobs.REL_TYPE );
            tx.success();
        }
        finally
        {
            tx.finish();
        }

        tx = haDb1.beginTx();
        // try throw in node that does not exist and no tx on mdb
        try
        {
            Node node = haDb1.createNode();
            mDb.getReferenceNode().createRelationshipTo( node, CommonJobs.KNOWS );
            fail( "Should throw NotFoundException" );
        }
        catch ( NotFoundException e )
        {
            // good
        }
        finally
        {
            tx.finish();
        }
    }
    
    private HighlyAvailableGraphDatabase getMasterHaDb()
    {
        PlaceHolderGraphDatabaseService db = (PlaceHolderGraphDatabaseService) getMaster().getGraphDb();
        return (HighlyAvailableGraphDatabase) db.getDb();
    }

    @Test
    public void slaveWriteThatOnlyModifyRelationshipRecordsCanUpdateCachedNodeOnMaster() throws Exception
    {
        initializeDbs( 1, MapUtil.stringMap( Config.CACHE_TYPE, "strong" ) );
        HighlyAvailableGraphDatabase sDb = (HighlyAvailableGraphDatabase) getSlave( 0 );
        HighlyAvailableGraphDatabase mDb = getMasterHaDb();

        long relId;
        Node node;

        Transaction tx = mDb.beginTx();
        try
        {
            node = mDb.createNode();
            // "pad" the relationship so that removing it doesn't update the node record
            node.createRelationshipTo( mDb.createNode(), REL_TYPE );
            relId = node.createRelationshipTo( mDb.createNode(), REL_TYPE ).getId();
            node.createRelationshipTo( mDb.createNode(), REL_TYPE );

            tx.success();
        }
        finally
        {
            tx.finish();
        }

        // update the slave to make getRelationshipById() work
        sDb.pullUpdates();

        // remove the relationship on the slave
        tx = sDb.beginTx();
        try
        {
            sDb.getRelationshipById( relId ).delete();

            tx.success();
        }
        finally
        {
            tx.finish();
        }

        // verify that the removed relationship is gone from the master
        int relCount = 0;
        for ( Relationship rel : node.getRelationships() )
        {
            rel.getOtherNode( node );
            relCount++;
        }
        assertEquals( "wrong number of relationships", 2, relCount );
    }
    
    @Test
    public void mastersMessagesLogShouldNotContainMentionsAboutAppliedTransactions() throws Exception
    {
        initializeDbs( 1 );
        for ( int i = 0; i < 5; i++ )
        {
            executeJob( new CommonJobs.CreateNodeJob(), 0 );
        }
        disableVerificationAfterTest();
        shutdownDbs();
        // Strings copied from XaLogicalLog#applyTransactionWithoutTxId
        Collection<String> toLookFor = asList( "applyTxWithoutTxId log version", "Applied external tx and generated" );
        assertEquals( 0, countMentionsInMessagesLog( dbPath( 0 ).getAbsolutePath(), toLookFor ) );
    }

    @Test
    public void halfWayCopyWithSuccessfulRetry() throws Exception
    {
        createBigMasterStore( 10 );
        startUpMaster( MapUtil.stringMap() );
        int slaveMachineId = addDb( MapUtil.stringMap(), false );
        awaitAllStarted();
        shutdownDb( slaveMachineId );
        
        // Simulate an uncompleted copy by removing the "neostore" file as well as
        // the relationship store file f.ex.
        FileUtils.deleteFiles( dbPath( slaveMachineId ), "nioneo.*\\.v.*" );
        FileUtils.deleteRecursively( new File( dbPath( slaveMachineId ), "index" ) );
        assertTrue( new File( dbPath( slaveMachineId ), "neostore" ).delete() );
        assertTrue( new File( dbPath( slaveMachineId ), "neostore.relationshipstore.db" ).delete() );
        File propertyStoreFile = new File( dbPath( slaveMachineId ), "neostore.propertystore.db" );
        FileUtils.truncateFile( propertyStoreFile, propertyStoreFile.length()/2 );
        
        // Start the db again so that a full copy can be made again. Verification is
        // done @After
        startDb( slaveMachineId, MapUtil.stringMap(), true );
        awaitAllStarted();
    }
    
    @Test
    public void failCommitLongGoingTxOnSlaveAfterMasterRestart() throws Exception
    {
        initializeDbs( 1 );
        
        // Create a node on master
        GraphDatabaseService master = getMaster().getGraphDb();
        Transaction masterTx = master.beginTx();
        long masterNodeId = master.createNode().getId();
        masterTx.success(); masterTx.finish();
        
        // Pull updates and begin tx on slave which sets a property on that node
        // and creates one other node. Don't commit yet 
        HighlyAvailableGraphDatabase slave = (HighlyAvailableGraphDatabase) getSlave( 0 );
        slave.pullUpdates();
        Transaction slaveTx = slave.beginTx();
        slave.getNodeById( masterNodeId ).setProperty( "key", "value" );
        slave.index().forNodes( "name" ).add( slave.getNodeById( masterNodeId ), "key", "value" );
        slave.createNode().getId();
        
        // Restart the master
        getMasterHaDb().shutdown();
        ((PlaceHolderGraphDatabaseService)getMaster().getGraphDb()).setDb(
                startUpMasterDb( MapUtil.stringMap() ).getDb() );
        
        // Try to commit the tx from the slave and make sure it cannot do that
        slaveTx.success();
        try
        {
            slaveTx.finish();
            fail( "Shouldn't be able to commit here" );
        }
        catch ( TransactionFailureException e ) { /* Good */ }
    }
}
