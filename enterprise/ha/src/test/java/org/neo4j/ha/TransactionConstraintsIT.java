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
package org.neo4j.ha;

import java.util.concurrent.Future;

import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.factory.GraphDatabaseBuilder;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.qa.tooling.DumpProcessInformationRule;
import org.neo4j.test.AbstractClusterTest;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;
import org.neo4j.test.ha.ClusterManager;

import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.neo4j.qa.tooling.DumpProcessInformationRule.localVm;
import static org.neo4j.test.ha.ClusterManager.masterAvailable;

public class TransactionConstraintsIT extends AbstractClusterTest
{
    @Test
    public void start_tx_as_slave_and_finish_it_after_having_switched_to_master_should_not_succeed() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = cluster.getAnySlave();
        takeTheLeadInAnEventualMasterSwitch( db );

        // WHEN
        Transaction tx = db.beginTx();
        try
        {
            db.createNode().setProperty( "name", "slave" );
            tx.success();
        }
        finally
        {
            cluster.shutdown( cluster.getMaster() );
            assertFinishGetsTransactionFailure( tx );
        }

        cluster.await( masterAvailable() );

        // THEN
        assertEquals( db, cluster.getMaster() );
        // to prevent a deadlock scenario which occurs if this test exists (and @After starts)
        // before the db has recovered from its KERNEL_PANIC
        awaitFullyOperational( db );
    }

    @Test
    public void start_tx_as_slave_and_finish_it_after_another_master_being_available_should_not_succeed() throws Exception
    {
        // GIVEN
        HighlyAvailableGraphDatabase db = cluster.getAnySlave();

        // WHEN
        Transaction tx = db.beginTx();
        try
        {
            db.getReferenceNode().setProperty( "name", "slave" );
            tx.success();
        }
        finally
        {
            HighlyAvailableGraphDatabase theOtherSlave = cluster.getAnySlave( db );
            takeTheLeadInAnEventualMasterSwitch( theOtherSlave );
            cluster.shutdown( cluster.getMaster() );
            assertFinishGetsTransactionFailure( tx );
        }

        cluster.await( ClusterManager.masterAvailable() );

        // THEN
        assertFalse( db.isMaster() );
        // to prevent a deadlock scenario which occurs if this test exists (and @After starts)
        // before the db has recovered from its KERNEL_PANIC
        awaitFullyOperational( db );
    }
    
    @Test
    public void slave_should_not_be_able_to_produce_an_invalid_transaction() throws Exception
    {
        // GIVEN
        HighlyAvailableGraphDatabase aSlave = cluster.getAnySlave();
        Node node = createMiniTree( aSlave );

        // WHEN
        Transaction tx = aSlave.beginTx();
        try
        {
            // Deleting this node isn't allowed since it still has relationships
            node.delete();
            tx.success();
        }
        finally
        {
            // THEN
            assertFinishGetsTransactionFailure( tx );
        }
    }

    @Test
    public void master_should_not_be_able_to_produce_an_invalid_transaction() throws Exception
    {
        // GIVEN
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        Node node = createMiniTree( master );

        // WHEN
        Transaction tx = master.beginTx();
        try
        {
            // Deleting this node isn't allowed since it still has relationships
            node.delete();
            tx.success();
        }
        finally
        {
            // THEN
            assertFinishGetsTransactionFailure( tx );
        }
    }
    
    @Test
    public void write_operation_on_slave_has_to_be_performed_within_a_transaction() throws Exception
    {
        // GIVEN
        HighlyAvailableGraphDatabase aSlave = cluster.getAnySlave();

        // WHEN
        try
        {
            aSlave.createNode();
            fail( "Shouldn't be able to do a write operation outside a transaction" );
        }
        catch ( NotInTransactionException e )
        {
            // THEN
        }
    }
    
    @Test
    public void write_operation_on_master_has_to_be_performed_within_a_transaction() throws Exception
    {
        // GIVEN
        HighlyAvailableGraphDatabase master = cluster.getMaster();

        // WHEN
        try
        {
            master.createNode();
            fail( "Shouldn't be able to do a write operation outside a transaction" );
        }
        catch ( NotInTransactionException e )
        {
            // THEN
        }
    }
    
    @Test
    public void slave_should_not_be_able_to_modify_node_deleted_on_master() throws Exception
    {
        // GIVEN
        // -- node created on slave
        HighlyAvailableGraphDatabase aSlave = cluster.getAnySlave();
        Node node = createNode( aSlave );
        // -- that node delete on master, but the slave doesn't see it yet
        deleteNode( cluster.getMaster(), node.getId() );

        // WHEN
        Transaction tx = aSlave.beginTx();
        try
        {
            node.setProperty( "name", "test" );
            fail( "Shouldn't be able to modify a node deleted on master" );
        }
        catch ( NotFoundException e )
        {
            // THEN
            // -- the transactions gotten back in the response should delete that node
        }
        finally
        {
            tx.finish();
        }
    }
    
    @Test
    public void deadlock_detection_involving_two_slaves() throws Exception
    {
        HighlyAvailableGraphDatabase slave1 = cluster.getAnySlave();
        deadlockDetectionBetween( slave1, cluster.getAnySlave( slave1 ) );
    }
    
    @Test
    public void deadlock_detection_involving_slave_and_master() throws Exception
    {
        deadlockDetectionBetween( cluster.getAnySlave(), cluster.getMaster() );
    }
    
    private void deadlockDetectionBetween( HighlyAvailableGraphDatabase slave1, HighlyAvailableGraphDatabase slave2 ) throws Exception
    {
        // GIVEN
        // -- two members acquiring a read lock on the same entity
        OtherThreadExecutor<HighlyAvailableGraphDatabase> thread2 = new OtherThreadExecutor<HighlyAvailableGraphDatabase>( "T2", slave2 );
        Transaction tx1 = slave1.beginTx();
        Transaction tx2 = thread2.execute( new BeginTx() );
        tx1.acquireReadLock( slave1.getReferenceNode() );
        thread2.execute( new AcquireReadLock( tx2, slave2.getReferenceNode() ) );
        // -- and one of them wanting (and awaiting) to upgrade its read lock to a write lock
        Future<Lock> writeLockFuture = thread2.executeDontWait( new AcquireWriteLock( tx2, slave2.getReferenceNode() ) );
        thread2.waitUntilThreadState( Thread.State.TIMED_WAITING, Thread.State.WAITING );
        
        try
        {
            // WHEN
            tx1.acquireWriteLock( slave1.getReferenceNode() );
            fail( "Deadlock exception should have been thrown" );
        }
        catch ( DeadlockDetectedException e )
        {
            // THEN -- deadlock should be avoided with this exception
        }
        finally
        {
            tx1.finish();
        }
        
        assertNotNull( writeLockFuture.get() );
        thread2.execute( new FinishTx( tx2, true ) );
        thread2.shutdown();
    }

    @Ignore( "Known issue where locks acquired from Transaction#acquireXXXLock() methods doesn't get properly released when calling Lock#release() method" )
    @Test
    public void manually_acquire_and_release_transaction_lock() throws Exception
    {
        // GIVEN
        // -- a slave acquiring a lock on an ubiquitous node
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        OtherThreadExecutor<HighlyAvailableGraphDatabase> masterWorker = new OtherThreadExecutor<HighlyAvailableGraphDatabase>( "master worker", master );
        Node node = createNode( master );
        cluster.sync();
        HighlyAvailableGraphDatabase slave = cluster.getAnySlave();
        Transaction slaveTx = slave.beginTx();
        try
        {
            Lock lock = slaveTx.acquireWriteLock( slave.getNodeById( node.getId() ) );
    
            // WHEN
            // -- the lock is manually released (tx still running)
            lock.release();
    
            // THEN
            // -- that entity should be able to be locked from another member
            Transaction masterTx = masterWorker.execute( new BeginTx() );
            masterWorker.execute( new AcquireWriteLock( masterTx, node ), 1, SECONDS );
        }
        finally
        {
            slaveTx.finish();
            masterWorker.shutdown();
        }
    }
    
    private void takeTheLeadInAnEventualMasterSwitch( GraphDatabaseService db )
    {
        createNode( db );
    }

    private Node createNode( GraphDatabaseService db )
    {
        Transaction tx = db.beginTx();
        try
        {
            Node node = db.createNode();
            node.setProperty( "name", "yo" );
            tx.success();
            return node;
        }
        finally
        {
            tx.finish();
        }
    }
    
    private Node createMiniTree( GraphDatabaseService db )
    {
        Transaction tx = db.beginTx();
        try
        {
            Node root = db.createNode();
            root.createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            root.createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            tx.success();
            return root;
        }
        finally
        {
            tx.finish();
        }
    }
    
    private void deleteNode( HighlyAvailableGraphDatabase db, long id )
    {
        Transaction tx = db.beginTx();
        try
        {
            db.getNodeById( id ).delete();
            tx.success();
        }
        finally
        {
            tx.finish();
        }
    }
    
    @Override
    protected void configureClusterMember( GraphDatabaseBuilder builder, String clusterName, int serverId )
    {
        super.configureClusterMember( builder, clusterName, serverId );
        builder.setConfig( HaSettings.tx_push_factor, "0" );
        builder.setConfig( HaSettings.pull_interval, "0" );
    }

    private void assertFinishGetsTransactionFailure( Transaction tx )
    {
        try
        {
            tx.finish();
            fail( "Transaction shouldn't be able to finish" );
        }
        catch ( TransactionFailureException e )
        {   // Good
        }
    }
    
    private static class AcquireReadLock implements WorkerCommand<HighlyAvailableGraphDatabase, Lock>
    {
        private final Transaction tx;
        private final Node node;

        public AcquireReadLock( Transaction tx, Node node )
        {
            this.tx = tx;
            this.node = node;
        }

        @Override
        public Lock doWork( HighlyAvailableGraphDatabase state )
        {
            return tx.acquireReadLock( node );
        }
    }

    private static class AcquireWriteLock implements WorkerCommand<HighlyAvailableGraphDatabase, Lock>
    {
        private final Transaction tx;
        private final Node node;

        public AcquireWriteLock( Transaction tx, Node node )
        {
            this.tx = tx;
            this.node = node;
        }

        @Override
        public Lock doWork( HighlyAvailableGraphDatabase state )
        {
            return tx.acquireWriteLock( node );
        }
    }

    @Rule
    public DumpProcessInformationRule dumpInfo = new DumpProcessInformationRule( 1, MINUTES, localVm( System.out ) );

    private void awaitFullyOperational( GraphDatabaseService db ) throws InterruptedException
    {
        while ( true )
        {
            try
            {
                doABogusTransaction( db );
                break;
            }
            catch ( Exception e )
            {
                Thread.sleep( 100 );
            }
        }
    }

    private void doABogusTransaction( GraphDatabaseService db ) throws Exception
    {
        Transaction tx = db.beginTx();
        try
        {
            db.createNode();
        }
        finally
        {
            tx.finish();
        }
    }
}
