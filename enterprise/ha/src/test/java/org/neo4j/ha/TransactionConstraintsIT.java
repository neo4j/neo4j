/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Rule;
import org.junit.Test;

import org.neo4j.graphdb.DynamicLabel;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.qa.tooling.DumpProcessInformationRule;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.neo4j.test.ha.ClusterManager;
import org.neo4j.test.ha.ClusterRule;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MINUTES;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import static org.neo4j.helpers.collection.IteratorUtil.single;
import static org.neo4j.qa.tooling.DumpProcessInformationRule.localVm;
import static org.neo4j.test.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.test.ha.ClusterManager.masterAvailable;

public class TransactionConstraintsIT
{
    @Rule
    public final ClusterRule clusterRule = new ClusterRule(getClass()).config(HaSettings.pull_interval, "0");

    protected ClusterManager.ManagedCluster cluster;

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule.startCluster( );
    }

    private static final String PROPERTY_KEY = "name";
    private static final String PROPERTY_VALUE = "yo";
    private static final String LABEL = "Person";

    @Test
    public void startTxAsSlaveAndFinishItAfterHavingSwitchedToMasterShouldNotSucceed() throws Exception
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
    public void startTxAsSlaveAndFinishItAfterAnotherMasterBeingAvailableShouldNotSucceed() throws Exception
    {
        // GIVEN
        HighlyAvailableGraphDatabase db = cluster.getAnySlave();

        // WHEN
        HighlyAvailableGraphDatabase theOtherSlave;
        Transaction tx = db.beginTx();
        try
        {
            db.createNode().setProperty( "name", "slave" );
            tx.success();
        }
        finally
        {
            theOtherSlave = cluster.getAnySlave( db );
            takeTheLeadInAnEventualMasterSwitch( theOtherSlave );
            cluster.shutdown( cluster.getMaster() );
            assertFinishGetsTransactionFailure( tx );
        }

        cluster.await( ClusterManager.masterAvailable() );

        // THEN
        assertFalse( db.isMaster() );
        assertTrue( theOtherSlave.isMaster() );
        // to prevent a deadlock scenario which occurs if this test exists (and @After starts)
        // before the db has recovered from its KERNEL_PANIC
        awaitFullyOperational( db );
    }

    @Test
    public void slaveShouldNotBeAbleToProduceAnInvalidTransaction() throws Exception
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
    public void masterShouldNotBeAbleToProduceAnInvalidTransaction() throws Exception
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
    public void writeOperationOnSlaveHasToBePerformedWithinTransaction() throws Exception
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
    public void writeOperationOnMasterHasToBePerformedWithinTransaction() throws Exception
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
    public void slaveShouldNotBeAbleToModifyNodeDeletedOnMaster() throws Exception
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
    public void deadlockDetectionInvolvingTwoSlaves() throws Exception
    {
        HighlyAvailableGraphDatabase slave1 = cluster.getAnySlave();
        deadlockDetectionBetween( slave1, cluster.getAnySlave( slave1 ) );
    }

    @Test
    public void deadlockDetectionInvolvingSlaveAndMaster() throws Exception
    {
        deadlockDetectionBetween( cluster.getAnySlave(), cluster.getMaster() );
    }

    private void deadlockDetectionBetween( HighlyAvailableGraphDatabase slave1, final HighlyAvailableGraphDatabase slave2 ) throws Exception
    {
        // GIVEN
        // -- two members acquiring a read lock on the same entity
        final Node commonNode;
        try(Transaction tx = slave1.beginTx())
        {
            commonNode = slave1.createNode();
            tx.success();
        }

        OtherThreadExecutor<HighlyAvailableGraphDatabase> thread2 = new OtherThreadExecutor<>( "T2", slave2 );
        Transaction tx1 = slave1.beginTx();
        Transaction tx2 = thread2.execute( new BeginTx() );
        tx1.acquireReadLock( commonNode );
        thread2.execute( new AcquireReadLockOnReferenceNode( tx2, commonNode ) );
        // -- and one of them wanting (and awaiting) to upgrade its read lock to a write lock
        Future<Lock> writeLockFuture = thread2.executeDontWait( new AcquireWriteLock( tx2, new Callable<Node>(){
            @Override
            public Node call() throws Exception
            {
                return commonNode;
            }
        } ) );

        for ( int i = 0; i < 10; i++ )
        {
            thread2.waitUntilThreadState( Thread.State.TIMED_WAITING, Thread.State.WAITING );
            Thread.sleep(2);
        }

        try
        {
            // WHEN
            tx1.acquireWriteLock( commonNode );

            // -- Deadlock detection is non-deterministic, so either the slave or the master will detect it
            writeLockFuture.get();
            fail( "Deadlock exception should have been thrown" );
        }
        catch ( DeadlockDetectedException e )
        {
            // THEN -- deadlock should be avoided with this exception
        }
        finally
        {
            tx1.close();
        }

        thread2.execute( new FinishTx( tx2, true ) );
        thread2.close();
    }

    @Test
    public void createdSchemaConstraintsMustBeRetainedAcrossModeSwitches() throws Throwable
    {
        // GIVEN
        // -- a node with a label and a property, and a constraint on those
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        createConstraint( master, LABEL, PROPERTY_KEY );
        createNode( master, LABEL ).getId();

        // WHEN
        cluster.sync();
        ClusterManager.RepairKit repairKit = cluster.fail( master );
        cluster.await( masterAvailable( master ) );
        takeTheLeadInAnEventualMasterSwitch( cluster.getMaster() );
        cluster.sync();

        // TODO There is a bug, where the master does not notice that its followers have vanished.
        // We have to do this sleep here to make sure that it times them out properly.
        // See https://trello.com/c/hcTvl0bv
        Thread.sleep( 30000 );

        repairKit.repair();
        cluster.await( allSeesAllAsAvailable() );
        cluster.sync();
        cluster.stop();

        // THEN
        // -- these fiddlings with the stores must not throw
        for ( HighlyAvailableGraphDatabase instance : cluster.getAllMembers() )
        {
            GraphDatabaseService db = new TestGraphDatabaseFactory().newEmbeddedDatabase( instance.getStoreDir() );
            Label label = DynamicLabel.label( LABEL );
            try ( Transaction tx = db.beginTx() )
            {
                Node node = db.createNode( label );
                node.setProperty( PROPERTY_KEY, PROPERTY_VALUE + "1" );
                tx.success();
            }
            try ( Transaction tx = db.beginTx() )
            {
                ConstraintDefinition constraint = single( db.schema().getConstraints( label ) );
                constraint.drop();
                tx.success();
            }
            db.shutdown();
        }
    }

    @Ignore( "Known issue where locks acquired from Transaction#acquireXXXLock() methods doesn't get properly released when calling Lock#release() method" )
    @Test
    public void manuallyAcquireAndReleaseTransactionLock() throws Exception
    {
        // GIVEN
        // -- a slave acquiring a lock on an ubiquitous node
        HighlyAvailableGraphDatabase master = cluster.getMaster();
        OtherThreadExecutor<HighlyAvailableGraphDatabase> masterWorker = new OtherThreadExecutor<>( "master worker", master );
        final Node node = createNode( master );
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
            masterWorker.execute( new AcquireWriteLock( masterTx, new Callable<Node>()
            {
                @Override
                public Node call() throws Exception
                {
                    return node;
                }
            } ), 1, SECONDS );
        }
        finally
        {
            slaveTx.finish();
            masterWorker.close();
        }
    }

    private void takeTheLeadInAnEventualMasterSwitch( GraphDatabaseService db )
    {
        createNode( db );
    }

    private Node createNode( GraphDatabaseService db, String... labels )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode();
            for ( String label : labels )
            {
                node.addLabel( DynamicLabel.label( label ) );
            }
            node.setProperty( PROPERTY_KEY, PROPERTY_VALUE );
            tx.success();
            return node;
        }
    }

    private void createConstraint( HighlyAvailableGraphDatabase db, String label, String propertyName )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( DynamicLabel.label( label ) ).assertPropertyIsUnique( propertyName ).create();
            tx.success();
        }
    }

    private Node createMiniTree( GraphDatabaseService db )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node root = db.createNode();
            root.createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            root.createRelationshipTo( db.createNode(), MyRelTypes.TEST );
            tx.success();
            return root;
        }
    }

    private void deleteNode( HighlyAvailableGraphDatabase db, long id )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.getNodeById( id ).delete();
            tx.success();
        }
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

    private static class AcquireReadLockOnReferenceNode implements WorkerCommand<HighlyAvailableGraphDatabase, Lock>
    {
        private final Transaction tx;
        private final Node commonNode;

        public AcquireReadLockOnReferenceNode( Transaction tx, Node commonNode )
        {
            this.tx = tx;
            this.commonNode = commonNode;
        }

        @Override
        public Lock doWork( HighlyAvailableGraphDatabase state )
        {
            return tx.acquireReadLock( commonNode );
        }
    }

    private static class AcquireWriteLock implements WorkerCommand<HighlyAvailableGraphDatabase, Lock>
    {
        private final Transaction tx;
        private final Callable<Node> callable;

        public AcquireWriteLock( Transaction tx, Callable<Node> callable )
        {
            this.tx = tx;
            this.callable = callable;
        }

        @Override
        public Lock doWork( HighlyAvailableGraphDatabase state ) throws Exception
        {
            return tx.acquireWriteLock( callable.call() );
        }
    }

    @Rule
    public DumpProcessInformationRule dumpInfo = new DumpProcessInformationRule( 1, MINUTES, localVm( System.out ) );

    private void awaitFullyOperational( GraphDatabaseService db ) throws InterruptedException
    {
        long endTime = currentTimeMillis() + MINUTES.toMillis( 1 );
        for ( int i = 0; currentTimeMillis() < endTime; i++ )
        {
            try
            {
                doABogusTransaction( db );
                break;
            }
            catch ( Exception e )
            {
                if ( i > 0 && i%10 == 0 )
                {
                    e.printStackTrace();
                }
                Thread.sleep( 1000 );
            }
        }
    }

    private void doABogusTransaction( GraphDatabaseService db ) throws Exception
    {
        try ( Transaction ignore = db.beginTx() )
        {
            db.createNode();
        }
    }
}
