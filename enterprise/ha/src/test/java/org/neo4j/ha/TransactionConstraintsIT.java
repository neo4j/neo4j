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
package org.neo4j.ha;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.neo4j.function.IntFunction;
import org.neo4j.function.Predicate;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Lock;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.NotFoundException;
import org.neo4j.graphdb.NotInTransactionException;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.TransientTransactionFailureException;
import org.neo4j.kernel.DeadlockDetectedException;
import org.neo4j.kernel.ha.HaSettings;
import org.neo4j.kernel.ha.HighlyAvailableGraphDatabase;
import org.neo4j.kernel.impl.MyRelTypes;
import org.neo4j.kernel.impl.ha.ClusterManager;
import org.neo4j.qa.tooling.DumpProcessInformationRule;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;
import org.neo4j.test.ha.ClusterRule;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MINUTES;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.DynamicLabel.label;
import static org.neo4j.helpers.collection.Iterables.filter;
import static org.neo4j.helpers.collection.IteratorUtil.single;
import static org.neo4j.kernel.impl.ha.ClusterManager.allSeesAllAsAvailable;
import static org.neo4j.kernel.impl.ha.ClusterManager.masterAvailable;
import static org.neo4j.qa.tooling.DumpProcessInformationRule.localVm;

public class TransactionConstraintsIT
{
    private static final int SLAVE_ONLY_ID = 1;

    @Rule
    public final ClusterRule clusterRule = new ClusterRule( getClass() )
            .withSharedSetting( HaSettings.pull_interval, "0" )
            .withInstanceSetting( HaSettings.slave_only, new IntFunction<String>()
            {
                @Override
                public String apply( int serverId )
                {
                    return serverId == SLAVE_ONLY_ID ? "true" : "false";
                }
            });

    protected ClusterManager.ManagedCluster cluster;

    @Before
    public void setup() throws Exception
    {
        cluster = clusterRule.startCluster();
    }

    private static final String PROPERTY_KEY = "name";
    private static final String PROPERTY_VALUE = "yo";
    private static final Label LABEL = label( "Person" );

    @Test
    public void startTxAsSlaveAndFinishItAfterHavingSwitchedToMasterShouldNotSucceed() throws Exception
    {
        // GIVEN
        GraphDatabaseService db = cluster.getAnySlave( getSlaveOnlySlave() );

        // WHEN
        Transaction tx = db.beginTx();
        try
        {
            db.createNode().setProperty( "name", "slave" );
            tx.success();
        }
        finally
        {
            HighlyAvailableGraphDatabase oldMaster = cluster.getMaster();
            cluster.shutdown( oldMaster );
            // Wait for new master
            cluster.await( masterAvailable( oldMaster ) );
            assertFinishGetsTransactionFailure( tx );
        }

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
        HighlyAvailableGraphDatabase db = getSlaveOnlySlave();
        HighlyAvailableGraphDatabase oldMaster;

        // WHEN
        Transaction tx = db.beginTx();
        try
        {
            db.createNode().setProperty( "name", "slave" );
            tx.success();
        }
        finally
        {
            oldMaster = cluster.getMaster();
            cluster.shutdown( oldMaster );
            // Wait for new master
            cluster.await( masterAvailable( oldMaster ) );
            // THEN
            assertFinishGetsTransactionFailure( tx );
        }

        assertFalse( db.isMaster() );
        assertFalse( oldMaster.isMaster() );
        // to prevent a deadlock scenario which occurs if this test exists (and @After starts)
        // before the db has recovered from its KERNEL_PANIC
        awaitFullyOperational( db );
    }

    private HighlyAvailableGraphDatabase getSlaveOnlySlave()
    {
        HighlyAvailableGraphDatabase db = single( filter( new Predicate<HighlyAvailableGraphDatabase>()
        {
            @Override
            public boolean test( HighlyAvailableGraphDatabase db )
            {
                return cluster.getServerId( db ).toIntegerIndex() == SLAVE_ONLY_ID;
            }
        }, cluster.getAllMembers() ) );
        assertFalse( db.isMaster() );
        return db;
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
        Node node = createNode( aSlave, PROPERTY_VALUE );
        // -- that node delete on master, but the slave doesn't see it yet
        deleteNode( cluster.getMaster(), node.getId() );

        // WHEN
        try (Transaction slaveTransaction = aSlave.beginTx())
        {
            node.setProperty( "name", "test" );
            fail( "Shouldn't be able to modify a node deleted on master" );
        }
        catch ( NotFoundException e )
        {
            // THEN
            // -- the transactions gotten back in the response should delete that node
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
        try ( Transaction tx = slave1.beginTx() )
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
        createNode( master, PROPERTY_VALUE, LABEL ).getId();

        // WHEN
        cluster.sync();
        ClusterManager.RepairKit originalMasterRepairKit = cluster.fail( master );
        cluster.await( masterAvailable( master ) );
        takeTheLeadInAnEventualMasterSwitch( cluster.getMaster() );
        cluster.sync();

        originalMasterRepairKit.repair();
        cluster.await( allSeesAllAsAvailable() );
        cluster.sync();

        // THEN the constraints should still be in place and enforced
        int i = 0;
        for ( HighlyAvailableGraphDatabase instance : cluster.getAllMembers() )
        {
            try
            {
                createNode( instance, PROPERTY_VALUE, LABEL );
                fail( "Node with " + PROPERTY_VALUE + " should already exist" );
            }
            catch ( ConstraintViolationException e )
            {
                // Good, this node should already exist
            }
            for ( int p = 0; p < i - 1; p++ )
            {
                try
                {
                    createNode( instance, PROPERTY_VALUE + String.valueOf( p ), LABEL );
                    fail( "Node with " + PROPERTY_VALUE + String.valueOf( p ) + " should already exist" );
                }
                catch ( ConstraintViolationException e )
                {
                    // Good
                }
            }

            createNode( instance, PROPERTY_VALUE + String.valueOf( i ), LABEL );
            i++;
        }
    }

    private void takeTheLeadInAnEventualMasterSwitch( GraphDatabaseService db )
    {
        createNode( db, PROPERTY_VALUE );
    }

    private Node createNode( GraphDatabaseService db, Object propertyValue, Label... labels )
    {
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( labels );
            node.setProperty( PROPERTY_KEY, propertyValue );
            tx.success();
            return node;
        }
    }

    private void createConstraint( HighlyAvailableGraphDatabase db, Label label, String propertyName )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.schema().constraintFor( label ).assertPropertyIsUnique( propertyName ).create();
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
            tx.close();
            fail( "Transaction shouldn't be able to finish" );
        }
        catch ( TransientTransactionFailureException | TransactionFailureException e )
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
