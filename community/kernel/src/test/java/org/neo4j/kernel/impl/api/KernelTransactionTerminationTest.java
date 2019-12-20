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
package org.neo4j.kernel.impl.api;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.neo4j.collection.Dependencies;
import org.neo4j.collection.pool.Pool;
import org.neo4j.configuration.Config;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.internal.index.label.LabelScanStore;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.internal.schema.SchemaState;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.database.TestDatabaseIdRepository;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.stats.IndexStatisticsStore;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.factory.CanWrite;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.locking.NoOpClient;
import org.neo4j.kernel.impl.locking.SimpleStatementLocks;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.internal.event.DatabaseTransactionEventListeners;
import org.neo4j.resources.CpuClock;
import org.neo4j.resources.HeapAllocation;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.test.Race;
import org.neo4j.time.Clocks;

import static java.lang.System.currentTimeMillis;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.neo4j.internal.kernel.api.connectioninfo.ClientConnectionInfo.EMBEDDED_CONNECTION;
import static org.neo4j.internal.kernel.api.security.SecurityContext.AUTH_DISABLED;
import static org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier.ON_HEAP;
import static org.neo4j.test.rule.DatabaseRule.mockedTokenHolders;

class KernelTransactionTerminationTest
{
    private static final int TEST_RUN_TIME_SECS = 5;

    @Test
    @Timeout( TEST_RUN_TIME_SECS * 20 )
    void transactionCantBeTerminatedAfterItIsClosed() throws Throwable
    {
        runTwoThreads(
            () -> {},
            tx -> tx.markForTermination( Status.Transaction.TransactionMarkedAsFailed ),
            tx ->
            {
                close( tx );
                assertFalse( tx.getReasonIfTerminated().isPresent() );
                tx.initialize();
            } );
    }

    @Test
    @Timeout( TEST_RUN_TIME_SECS * 20 )
    void closeTransaction() throws Throwable
    {
        BlockingQueue<Boolean> committerToTerminator = new LinkedBlockingQueue<>( 1 );
        BlockingQueue<TerminatorAction> terminatorToCommitter = new LinkedBlockingQueue<>( 1 );
        AtomicBoolean t1Done = new AtomicBoolean();

        runTwoThreads(
            () ->
            {
                committerToTerminator.clear();
                terminatorToCommitter.clear();
                t1Done.set( false );
            },
            tx ->
            {
                Boolean terminatorShouldAct = committerToTerminator.poll();
                if ( terminatorShouldAct != null && terminatorShouldAct )
                {
                    TerminatorAction action = TerminatorAction.random();
                    action.executeOn( tx );
                    assertTrue( terminatorToCommitter.add( action ) );
                }
                t1Done.set( true );
            },
            tx ->
            {
                CommitterAction committerAction = CommitterAction.random();
                if ( committerToTerminator.offer( true ) )
                {
                    TerminatorAction terminatorAction = null;
                    try
                    {
                        // This loop optimizes the wait instead of waiting potentially a long time for T1 when it would lose the race and not do anything
                        while ( !t1Done.get() && terminatorAction == null )
                        {
                            terminatorAction = terminatorToCommitter.poll( 10, MILLISECONDS );
                        }
                    }
                    catch ( InterruptedException e )
                    {
                        Thread.currentThread().interrupt();
                        return;
                    }
                    if ( terminatorAction != null )
                    {
                        close( tx, committerAction, terminatorAction );
                    }
                }
            }
        );
    }

    private void runTwoThreads(
            Runnable cleaner,
            Consumer<TestKernelTransaction> thread1Action,
            Consumer<TestKernelTransaction> thread2Action ) throws Throwable
    {
        TestKernelTransaction tx = TestKernelTransaction.create();
        long endTime = currentTimeMillis() + SECONDS.toMillis( TEST_RUN_TIME_SECS );
        int limit = 20_000;
        for ( int i = 0; i < limit && currentTimeMillis() < endTime; i++ )
        {
            cleaner.run();
            tx.initialize();
            Race race = new Race().withRandomStartDelays( 0, 10 );
            race.withEndCondition( () -> currentTimeMillis() >= endTime );
            race.addContestant( () -> thread1Action.accept( tx ), 1 );
            race.addContestant( () -> thread2Action.accept( tx ), 1 );
            race.go();
        }
    }

    private static void close( KernelTransaction tx )
    {
        try
        {
            tx.close();
        }
        catch ( TransactionFailureException e )
        {
            throw new RuntimeException( e );
        }
    }

    private static void close( TestKernelTransaction tx, CommitterAction committer, TerminatorAction terminator )
    {
        try
        {
            if ( terminator == TerminatorAction.NONE )
            {
                committer.closeNotTerminated( tx );
            }
            else
            {
                committer.closeTerminated( tx );
            }
        }
        catch ( TransactionFailureException e )
        {
            throw new RuntimeException( e );
        }
    }

    private enum TerminatorAction
    {
        NONE
                {
                    @Override
                    void executeOn( KernelTransaction tx )
                    {
                    }
                },
        TERMINATE
                {
                    @Override
                    void executeOn( KernelTransaction tx )
                    {
                        tx.markForTermination( Status.Transaction.TransactionMarkedAsFailed );
                    }
                };

        abstract void executeOn( KernelTransaction tx );

        static TerminatorAction random()
        {
            return ThreadLocalRandom.current().nextBoolean() ? TERMINATE : NONE;
        }
    }

    private enum CommitterAction
    {
        NONE
                {
                    @Override
                    void closeTerminated( TestKernelTransaction tx ) throws TransactionFailureException
                    {
                        tx.assertTerminated();
                        tx.close();
                        tx.assertRolledBack();
                    }

                    @Override
                    void closeNotTerminated( TestKernelTransaction tx ) throws TransactionFailureException
                    {
                        tx.assertNotTerminated();
                        tx.close();
                        tx.assertRolledBack();
                    }
                },
        MARK_SUCCESS
                {
                    @Override
                    void closeTerminated( TestKernelTransaction tx )
                    {
                        tx.assertTerminated();
                        assertThrows( TransactionTerminatedException.class, tx::commit );
                        tx.assertRolledBack();
                    }

                    @Override
                    void closeNotTerminated( TestKernelTransaction tx ) throws TransactionFailureException
                    {
                        tx.assertNotTerminated();
                        tx.commit();
                        tx.assertCommitted();
                    }
                },
        MARK_FAILURE
                {
                    @Override
                    void closeTerminated( TestKernelTransaction tx ) throws TransactionFailureException
                    {
                        NONE.closeTerminated( tx );
                    }

                    @Override
                    void closeNotTerminated( TestKernelTransaction tx ) throws TransactionFailureException
                    {
                        NONE.closeNotTerminated( tx );
                    }
                };

        static final CommitterAction[] VALUES = values();

        abstract void closeTerminated( TestKernelTransaction tx ) throws TransactionFailureException;

        abstract void closeNotTerminated( TestKernelTransaction tx ) throws TransactionFailureException;

        static CommitterAction random()
        {
            return VALUES[ThreadLocalRandom.current().nextInt( VALUES.length )];
        }
    }

    private static class TestKernelTransaction extends KernelTransactionImplementation
    {
        final CommitTrackingMonitor monitor;

        TestKernelTransaction( CommitTrackingMonitor monitor, Dependencies dependencies )
        {
            super( Config.defaults(), mock( DatabaseTransactionEventListeners.class ),
                    mock( ConstraintIndexCreator.class ), mock( GlobalProcedures.class ),
                    mock( TransactionCommitProcess.class ), monitor, mock( Pool.class ), Clocks.fakeClock(),
                    new AtomicReference<>( CpuClock.NOT_AVAILABLE ), new AtomicReference<>( HeapAllocation.NOT_AVAILABLE ),
                    mock( DatabaseTracers.class, RETURNS_MOCKS ), mock( StorageEngine.class, RETURNS_MOCKS ), new CanWrite(),
                    EmptyVersionContextSupplier.EMPTY, ON_HEAP, new StandardConstraintSemantics(), mock( SchemaState.class ),
                    mockedTokenHolders(), mock( IndexingService.class ), mock( LabelScanStore.class ), mock( IndexStatisticsStore.class ), dependencies,
                    new TestDatabaseIdRepository().defaultDatabase(), LeaseService.NO_LEASES );

            this.monitor = monitor;
        }

        static TestKernelTransaction create()
        {
            Dependencies dependencies = new Dependencies();
            dependencies.satisfyDependency( mock( GraphDatabaseFacade.class ) );
            return new TestKernelTransaction( new CommitTrackingMonitor(), dependencies );
        }

        TestKernelTransaction initialize()
        {
            initialize( 42, 42, new SimpleStatementLocks( new NoOpClient() ), Type.IMPLICIT, AUTH_DISABLED, 0L, 1L, EMBEDDED_CONNECTION );
            monitor.reset();
            return this;
        }

        void assertCommitted()
        {
            assertTrue( monitor.committed );
        }

        void assertRolledBack()
        {
            assertTrue( monitor.rolledBack );
        }

        void assertTerminated()
        {
            assertEquals( Status.Transaction.TransactionMarkedAsFailed, getReasonIfTerminated().get() );
            assertTrue( monitor.terminated );
        }

        void assertNotTerminated()
        {
            assertFalse( getReasonIfTerminated().isPresent() );
            assertFalse( monitor.terminated );
        }
    }

    private static class CommitTrackingMonitor implements TransactionMonitor
    {
        volatile boolean committed;
        volatile boolean rolledBack;
        volatile boolean terminated;

        @Override
        public void transactionStarted()
        {
        }

        @Override
        public void transactionFinished( boolean successful, boolean writeTx )
        {
            if ( successful )
            {
                committed = true;
            }
            else
            {
                rolledBack = true;
            }
        }

        @Override
        public void transactionTerminated( boolean writeTx )
        {
            terminated = true;
        }

        @Override
        public void upgradeToWriteTransaction()
        {
        }

        void reset()
        {
            committed = false;
            rolledBack = false;
            terminated = false;
        }
    }
}
