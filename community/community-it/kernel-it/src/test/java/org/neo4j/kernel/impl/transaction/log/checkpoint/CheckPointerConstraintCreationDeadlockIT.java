/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.ConstraintViolationException;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.ConstraintDefinition;
import org.neo4j.kernel.impl.api.TransactionCommitProcess;
import org.neo4j.kernel.impl.api.TransactionToApply;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.store.kvstore.LockWrapper;
import org.neo4j.kernel.impl.store.kvstore.UpdateLock;
import org.neo4j.kernel.impl.transaction.TransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.TransactionCursor;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.logging.Logger;
import org.neo4j.monitoring.Monitors;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.Barrier;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.TestLabels;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.actors.Actor;
import org.neo4j.test.extension.actors.ActorsExtension;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.internal.helpers.collection.Iterables.single;
import static org.neo4j.kernel.impl.transaction.tracing.CommitEvent.NULL;
import static org.neo4j.storageengine.api.TransactionApplicationMode.EXTERNAL;

/**
 * The scenario, which takes place on database instance applying constraint
 * creation as an external transaction, looks like this:
 *
 * <ol>
 * <li>
 * Transaction T1 creates the constraint index and population P starts
 * </li>
 * <li>
 * Transaction T2 which activates the constraint starts applying and now has a read lock on the counts store
 * </li>
 * <li>
 * Check point triggers, wants to rotate counts store and so acquires its write lock.
 * It will have to block, but doing so will also blocks further read lock requests
 * </li>
 * <li>
 * T2 moves on to activate the constraint. Doing so means first waiting for the index to come online
 * </li>
 * <li>
 * P moves on to flip after population, something which includes initializing some sample data in counts store
 * for this index. Will block on the counts store read lock, completing the deadlock
 * </li>
 * </ol>
 */
@ActorsExtension
class CheckPointerConstraintCreationDeadlockIT
{
    private static final Label LABEL = TestLabels.LABEL_ONE;
    private static final String KEY = "key";

    @Inject
    Actor t2;
    @Inject
    Actor t3;

    @Test
    void shouldNotDeadlock() throws Exception
    {
        List<TransactionRepresentation> transactions = createConstraintCreatingTransactions();
        Monitors monitors = new Monitors();
        DatabaseManagementService managementService =
                new TestDatabaseManagementServiceBuilder().setMonitors( monitors ).impermanent().build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        Barrier.Control controller = new Barrier.Control();
        boolean success = false;
        try
        {
            IndexingService.Monitor monitor = new IndexingService.MonitorAdapter()
            {
                @Override
                public void indexPopulationScanComplete()
                {
                    controller.reached();
                }
            };
            monitors.addMonitorListener( monitor );
            Future<Object> applier = applyInT2( db, transactions );

            controller.await();

            // At this point the index population has completed and the population thread is ready to
            // acquire the counts store read lock for initializing some samples there. We're starting the
            // check pointer, which will eventually put itself in queue for acquiring the write lock

            Future<Object> checkPointer = t3.submit( () ->
                    db.getDependencyResolver().resolveDependency( CheckPointer.class )
                            .forceCheckPoint( new SimpleTriggerInfo( "MANUAL" ) ) );
            try
            {
                t3.untilWaitingIn( LockWrapper.class.getDeclaredMethod( "writeLock", UpdateLock.class, Logger.class ) );
            }
            catch ( IllegalStateException e )
            {
                // Thrown when the fix is in, basically it's thrown if the check pointer didn't get blocked
                checkPointer.get(); // to assert that no exception was thrown during in check point thread
            }

            // Alright the trap is set. Let the population thread move on and seal the deal
            controller.release();

            // THEN these should complete
            applier.get( 10, SECONDS );
            checkPointer.get( 10, SECONDS );
            success = true;

            try ( Transaction tx = db.beginTx() )
            {
                ConstraintDefinition constraint = single( db.schema().getConstraints( LABEL ) );
                assertEquals( KEY, single( constraint.getPropertyKeys() ) );
                tx.commit();
            }

            createNode( db, "A" );
            assertThrows( ConstraintViolationException.class, () -> createNode( db, "A" ) );
        }
        finally
        {
            if ( !success )
            {
                t2.interrupt();
                t3.interrupt();
                // so that shutdown won't hang too
            }
            managementService.shutdown();
        }
    }

    private void createNode( GraphDatabaseAPI db, String name )
    {
        try ( Transaction tx = db.beginTx() )
        {
            db.createNode( LABEL ).setProperty( KEY, name );
            tx.commit();
        }
    }

    private Future<Object> applyInT2( GraphDatabaseAPI db, List<TransactionRepresentation> transactions )
    {
        TransactionCommitProcess commitProcess =
                db.getDependencyResolver().resolveDependency( TransactionCommitProcess.class );
        return t2.submit( () ->
        {
            transactions.forEach( tx ->
            {
                // It will matter if the transactions are supplied all in the same batch or one by one
                // since the CountsTracker#apply lock is held and released per transaction
                assertDoesNotThrow( () -> commitProcess.commit( new TransactionToApply( tx ), NULL, EXTERNAL ) );
            } );
            return null;
        } );
    }

    private static List<TransactionRepresentation> createConstraintCreatingTransactions() throws Exception
    {
        DatabaseManagementService managementService = new TestDatabaseManagementServiceBuilder().impermanent().build();
        GraphDatabaseAPI db = (GraphDatabaseAPI) managementService.database( DEFAULT_DATABASE_NAME );
        try
        {
            try ( Transaction tx = db.beginTx() )
            {
                db.schema().constraintFor( LABEL ).assertPropertyIsUnique( KEY ).create();
                tx.commit();
            }

            LogicalTransactionStore txStore = db.getDependencyResolver().resolveDependency( LogicalTransactionStore.class );
            List<TransactionRepresentation> result = new ArrayList<>();
            try ( TransactionCursor cursor = txStore.getTransactions( TransactionIdStore.BASE_TX_ID + 1 ) )
            {
                while ( cursor.next() )
                {
                    result.add( cursor.get().getTransactionRepresentation() );
                }
            }
            return result;
        }
        finally
        {
            managementService.shutdown();
        }
    }
}
