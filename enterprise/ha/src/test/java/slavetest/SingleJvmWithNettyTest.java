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
package slavetest;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.helpers.collection.MapUtil.stringMap;
import static org.neo4j.kernel.HaConfig.CONFIG_KEY_LOCK_READ_TIMEOUT;
import static org.neo4j.kernel.HaConfig.CONFIG_KEY_READ_TIMEOUT;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.rmi.RemoteException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.neo4j.com.Client;
import org.neo4j.com.Client.ConnectionLostHandler;
import org.neo4j.com.Protocol;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.Config;
import org.neo4j.kernel.ConfigProxy;
import org.neo4j.kernel.GraphDatabaseSPI;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.ha.AbstractBroker;
import org.neo4j.kernel.ha.Broker;
import org.neo4j.kernel.ha.Master;
import org.neo4j.kernel.ha.MasterClient;
import org.neo4j.kernel.ha.MasterImpl;
import org.neo4j.kernel.ha.zookeeper.AbstractZooKeeperManager;
import org.neo4j.kernel.ha.zookeeper.Machine;
import org.neo4j.kernel.impl.nioneo.store.NeoStore;
import org.neo4j.kernel.impl.transaction.LockType;
import org.neo4j.kernel.impl.transaction.TxManager;
import org.neo4j.kernel.impl.util.FileUtils;
import org.neo4j.kernel.impl.util.StringLogger;

public class SingleJvmWithNettyTest extends SingleJvmTest
{
    private volatile Pair<Master, Machine> cachedMasterOverride;

    @Before
    public void setUp()
    {
        cachedMasterOverride = null;
    }

    @Test
    public void assertThatNettyIsUsed() throws Exception
    {
        initializeDbs( 1 );
        assertTrue(
                "Slave Broker is not a client",
                ( (HighlyAvailableGraphDatabase) getSlave( 0 ) ).getBroker().getMaster().first() instanceof MasterClient );
    }

    @Override
    protected Broker makeSlaveBroker( TestMaster master, int masterId, int id, HighlyAvailableGraphDatabase db, Map<String, String> config )
    {
        config.put( "server_id", Integer.toString( id ) );
        AbstractBroker.Configuration conf = ConfigProxy.config( config, AbstractBroker.Configuration.class );
        
        final Machine masterMachine = new Machine( masterId, -1, 1, -1,
                "localhost:" + Protocol.PORT );
        int readTimeout = getConfigInt( config, CONFIG_KEY_READ_TIMEOUT, TEST_READ_TIMEOUT );
        final Master client = new MasterClient(
                masterMachine.getServer().first(),
                masterMachine.getServer().other(),
                db.getMessageLog(),
                db.getStoreIdGetter(),
                ConnectionLostHandler.NO_ACTION,
                readTimeout, getConfigInt( config, CONFIG_KEY_LOCK_READ_TIMEOUT, readTimeout ),
                Client.DEFAULT_MAX_NUMBER_OF_CONCURRENT_CHANNELS_PER_CLIENT);
        return new AbstractBroker( conf )
        {
            public boolean iAmMaster()
            {
                return false;
            }

            public Pair<Master, Machine> getMasterReally( boolean allowChange )
            {
                if ( allowChange ) cachedMasterOverride = null;
                return getMasterPair();
            }

            public Pair<Master, Machine> getMaster()
            {
                return cachedMasterOverride != null ? cachedMasterOverride : getMasterPair();
            }

            private Pair<Master, Machine> getMasterPair()
            {
                return Pair.of( client, masterMachine );
            }

            public Object instantiateMasterServer( GraphDatabaseSPI graphDb )
            {
                throw new UnsupportedOperationException(
                        "cannot instantiate master server on slave" );
            }
        };
    }

    private int getConfigInt( Map<String, String> config, String key, int defaultValue )
    {
        String value = config.get( key );
        return value != null ? Integer.parseInt( value ) : defaultValue;
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

        /*
         * We might get 2, we might get 3 new channels, depending on the race between one thread releasing
         * and the other acquiring (if the first wins we get 2, if the second 3). Anyways, it must be more than 1.
         */
        assertTrue(
                "Did not get enough \"Opened a new channel\" log statements, something went missing",
                countOccurences( "Opened a new channel", new File(
                dbPath( 1 ), StringLogger.DEFAULT_NAME ) ) > 1 );
    }

    private int countOccurences( String string, File file ) throws Exception
    {
        BufferedReader reader = new BufferedReader( new FileReader( file ) );
        String line = null;
        int counter = 0;
        while ( (line = reader.readLine()) != null )
        {
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
        return (HighlyAvailableGraphDatabase) getMaster().getGraphDb();
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
        assertEquals( 0, countMentionsInMessagesLog( new File( dbPath( 0 ), StringLogger.DEFAULT_NAME ), toLookFor ) );
    }

    private int countMentionsInMessagesLog( File file, Collection<String> toLookFor )
    {
        int counter = 0;
        for ( String line : IteratorUtil.asIterable( file ) )
        {
            for ( String lookFor : toLookFor )
            {
                if ( line.contains( lookFor ) ) counter++;
            }
        }
        return counter;
    }

    @Test
    public void halfWayCopyWithSuccessfulRetry() throws Exception
    {
        startUpMaster( MapUtil.stringMap() );
        createBigMasterStore( 10 );
        int slaveMachineId = addDb( MapUtil.stringMap(), false );
        awaitAllStarted();
        shutdownDb( slaveMachineId );

        // Simulate an uncompleted copy by removing the "neostore" file as well as
        // the relationship store file f.ex.
        FileUtils.deleteFiles( dbPath( slaveMachineId ), "nioneo.*\\.v.*" );
        FileUtils.deleteRecursively( new File( dbPath( slaveMachineId ), "index" ) );
        assertTrue( new File( dbPath( slaveMachineId ), NeoStore.DEFAULT_NAME ).delete() );
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
        long slaveNodeId = slave.createNode().getId();

        // Restart the master
        getMasterHaDb().shutdown();
        HighlyAvailableGraphDatabase newMaster = startUpMasterDb( MapUtil.stringMap() );
        getMaster().setGraphDb( newMaster );
        

        // Try to commit the tx from the slave and make sure it cannot do that
        slaveTx.success();
        try
        {
            slaveTx.finish();
            fail( "Shouldn't be able to commit here" );
        }
        catch ( TransactionFailureException e ) { /* Good */ }

        assertNull( slave.getNodeById( masterNodeId ).getProperty( "key", null ) );
        try
        {
            slave.getNodeById( slaveNodeId );
        }
        catch ( NotFoundException e ) { /* Good */ }
    }

    @Test
    public void committsAndRollbacksCountCorrectlyOnMaster() throws Exception
    {
        initializeDbs( 1 );
        GraphDatabaseSPI master = getMaster().getGraphDb();
        GraphDatabaseSPI slave = getSlave( 0 );

        // A successful tx on the master should increment number of commits on master
        Pair<Integer, Integer> masterTxsBefore = getTransactionCounts( master );
        executeJobOnMaster( new CommonJobs.CreateNodeJob() );
        assertEquals( Pair.of( masterTxsBefore.first()+1, masterTxsBefore.other() ), getTransactionCounts( master ) );

        // A successful tx on slave should increment number of commits on master and slave
        masterTxsBefore = getTransactionCounts( master );
        Pair<Integer, Integer> slaveTxsBefore = getTransactionCounts( slave );
        executeJob( new CommonJobs.CreateNodeJob(), 0 );
        assertEquals( Pair.of( masterTxsBefore.first()+1, masterTxsBefore.other() ), getTransactionCounts( master ) );
        assertEquals( Pair.of( slaveTxsBefore.first()+1, slaveTxsBefore.other() ), getTransactionCounts( slave ) );

        // An unsuccessful tx on master should increment number of rollbacks on master
        masterTxsBefore = getTransactionCounts( master );
        executeJobOnMaster( new CommonJobs.CreateNodeJob( false ) );
        assertEquals( Pair.of( masterTxsBefore.first(), masterTxsBefore.other()+1 ), getTransactionCounts( master ) );

        // An unsuccessful tx on slave should increment number of rollbacks on master and slave
        masterTxsBefore = getTransactionCounts( master );
        slaveTxsBefore = getTransactionCounts( slave );
        executeJob( new CommonJobs.CreateNodeJob( false ), 0 );
        assertEquals( Pair.of( masterTxsBefore.first(), masterTxsBefore.other()+1 ), getTransactionCounts( master ) );
        assertEquals( Pair.of( slaveTxsBefore.first(), slaveTxsBefore.other()+1 ), getTransactionCounts( slave ) );
    }

    @Test
    public void individuallyConfigurableLockReadTimeout() throws Exception
    {
        long lockTimeout = 1;
        initializeDbs( 1, stringMap( CONFIG_KEY_LOCK_READ_TIMEOUT, String.valueOf( lockTimeout ) ) );
        final Long nodeId = executeJobOnMaster( new CommonJobs.CreateNodeJob( true ) );
        final Fetcher<DoubleLatch> latchFetcher = getDoubleLatch();
        pullUpdates();

        // Hold lock on master
        Thread lockHolder = new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    executeJobOnMaster( new CommonJobs.HoldLongLock( nodeId, latchFetcher ) );
                }
                catch ( Exception e )
                {
                    throw new RuntimeException( e );
                }
            }
        } );
        lockHolder.start();
        DoubleLatch latch = latchFetcher.fetch();
        latch.awaitFirst();

        // Try to get it on slave (should fail)
        long waitStart = System.currentTimeMillis();
        assertFalse( executeJob( new CommonJobs.SetNodePropertyJob( nodeId, "key", "value" ), 0 ) );
        long waitTime = System.currentTimeMillis()-waitStart;
        // Asserting time spent in a unit test is error prone. Comparing lockTimeout=1
        // against the default (20) / 2 = 10 should be pretty fine and should still verify
        // the correct behavior.
        assertTrue( "" + waitTime, waitTime < Client.DEFAULT_READ_RESPONSE_TIMEOUT_SECONDS*1000/2 );
        latch.countDownSecond();
    }

    @Test
    public void useLockTimeoutForCleaningUpTransactions() throws Exception
    {
        final long lockTimeout = 1;
        initializeDbs( 1, stringMap( CONFIG_KEY_LOCK_READ_TIMEOUT, String.valueOf( lockTimeout ) ) );
        final Long nodeId = executeJobOnMaster( new CommonJobs.CreateNodeJob( true ) );
        final Fetcher<DoubleLatch> latchFetcher = getDoubleLatch();
        pullUpdates();

        Thread lockHolder = new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                DoubleLatch latch = latchFetcher.fetch();
                try
                {
                    latch.awaitFirst();
                    Thread.sleep( ( lockTimeout + MasterImpl.UNFINISHED_TRANSACTION_CLEANUP_DELAY ) * 1000 );
                    latch.countDownSecond();
                }
                catch ( Exception e )
                {
                    throw new RuntimeException( e );
                }
            }
        } );
        lockHolder.start();

        try
        {
            executeJob( new CommonJobs.HoldLongLock( nodeId, latchFetcher ), 0 );
            fail( "Should have cleaned up transaction and thrown exception." );
        }
        catch ( TransactionFailureException e )
        {
        }
    }

    @Test
    public void useLockTimeoutToPreventCleaningUpLongRunningTransactions() throws Exception
    {
        final long lockTimeout = 100;
        initializeDbs( 1, stringMap( CONFIG_KEY_LOCK_READ_TIMEOUT, String.valueOf( lockTimeout ) ) );
        final Long nodeId = executeJobOnMaster( new CommonJobs.CreateNodeJob( true ) );
        final Fetcher<DoubleLatch> latchFetcher = getDoubleLatch();
        pullUpdates();

        Thread lockHolder = new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                DoubleLatch latch = latchFetcher.fetch();
                try
                {
                    latch.awaitFirst();
                    Thread.sleep( MasterImpl.UNFINISHED_TRANSACTION_CLEANUP_DELAY*2 * 1000 );
                    latch.countDownSecond();
                }
                catch ( Exception e )
                {
                    throw new RuntimeException( e );
                }
            }
        } );
        lockHolder.start();

        executeJob( new CommonJobs.HoldLongLock( nodeId, latchFetcher ), 0 );
    }

    @Ignore
    @Test
    public void readLockWithoutTxOnSlaveShouldNotGrabIndefiniteLockOnMaster() throws Exception
    {
        final long lockTimeout = 1;
        initializeDbs( 1, stringMap( CONFIG_KEY_LOCK_READ_TIMEOUT, String.valueOf( lockTimeout ) ) );
        final long[] id = new long[1];
        final Fetcher<DoubleLatch> latchFetcher = getDoubleLatch();
        Thread lockHolder = new Thread( new Runnable()
        {
            @Override
            public void run()
            {
                DoubleLatch latch = latchFetcher.fetch();
                try
                {
                    final GraphDatabaseService slaveDb = getSlave( 0 );
                    Node node;
                    Transaction tx = slaveDb.beginTx();
                    try
                    {
                        node = slaveDb.createNode();
                        tx.success();
                    }
                    finally
                    {
                        tx.finish();
                    }
                    ( (GraphDatabaseSPI) slaveDb ).getLockManager().getReadLock( node );
                    ( (GraphDatabaseSPI) slaveDb ).getLockReleaser().addLockToTransaction( node, LockType.READ );
                    id[0] = node.getId();
                    latch.countDownFirst();
                    latch.awaitSecond();
                }
                catch ( Exception e )
                {
                    throw new RuntimeException( e );
                }
                finally
                {
                    try
                    {
                        latch.countDownFirst();
                        latch.awaitSecond();
                    }
                    catch ( RemoteException e )
                    {
                        e.printStackTrace();
                    }
                }
            }
        }, "slaveLockHolder" );
        lockHolder.start();
        final DoubleLatch latch = latchFetcher.fetch();
        latch.awaitFirst();
        final HighlyAvailableGraphDatabase masterDb = getMasterHaDb();
        final Transaction tx = masterDb.beginTx();
        try
        {
            long startTime = System.currentTimeMillis();
            masterDb.getNodeById(id[0]).setProperty( "name", "David" );
            long duration = System.currentTimeMillis() - startTime;
            latch.countDownSecond();
            assertTrue( "Read lock was acquired but not released.", duration < lockTimeout*1000 );
        }
        finally
        {
            tx.finish();
        }
    }

    @Test
    public void indexPutIfAbsent() throws Exception
    {
        initializeDbs( 2 );
        long node = executeJobOnMaster( new CommonJobs.CreateNodeJob( true ) );
        pullUpdates();

        Worker t1 = new Worker( getSlave( 0 ) );
        Worker t2 = new Worker( getSlave( 1 ) );
        t1.beginTx();
        t2.beginTx();
        String index = "index";
        String key = "key";
        String value = "Mattias";
        assertNull( t2.putIfAbsent( index, node, key, value ).get() );
        Future<Node> futurePut = t1.putIfAbsent( index, node, key, value );
        t1.waitUntilWaiting();
        t2.finishTx( true );
        assertNotNull( futurePut.get() );
        t1.finishTx( true );

        assertEquals( node, getSlave( 0 ).index().forNodes( index ).get( key, value ).getSingle().getId() );
        assertEquals( node, getSlave( 1 ).index().forNodes( index ).get( key, value ).getSingle().getId() );

        t1.shutdown();
        t2.shutdown();
    }

    @Test
    public void pullUpdatesDoesNewMasterWhenThereIsNoMaster() throws Exception
    {
        disableVerificationAfterTest();
        initializeDbs( 2 );
        executeJob( new CommonJobs.CreateNodeJob( true ), 0 );

        cachedMasterOverride = AbstractZooKeeperManager.NO_MASTER_MACHINE_PAIR;
        getMaster().shutdown();
        int exceptionCount = 0;
        for ( int i = 0; i < 3; i++ )
        {
            try
            {
                pullUpdates( 1 );
            }
            catch ( Exception e )
            {
                exceptionCount++;
                e.printStackTrace();
            }
        }
        if (exceptionCount > 1)
        {
            fail( "Should not have gotten more than one failed pullUpdates during master switch." );
        }
    }

    private Pair<Integer, Integer> getTransactionCounts( GraphDatabaseSPI master )
    {
        return Pair.of(
            ( (TxManager) master.getTxManager() ).getCommittedTxCount(),
            ((TxManager)master.getTxManager()).getRolledbackTxCount() );
    }
}
