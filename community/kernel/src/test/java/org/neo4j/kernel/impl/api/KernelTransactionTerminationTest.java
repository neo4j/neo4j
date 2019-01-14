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

import org.junit.Test;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.neo4j.collection.pool.Pool;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.internal.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracerSupplier;
import org.neo4j.io.pagecache.tracing.cursor.context.EmptyVersionContextSupplier;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.explicitindex.AutoIndexing;
import org.neo4j.kernel.api.txstate.ExplicitIndexTransactionState;
import org.neo4j.kernel.impl.api.index.IndexProviderMap;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.factory.CanWrite;
import org.neo4j.kernel.impl.index.ExplicitIndexStore;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.locking.NoOpClient;
import org.neo4j.kernel.impl.locking.SimpleStatementLocks;
import org.neo4j.kernel.impl.newapi.DefaultCursors;
import org.neo4j.kernel.impl.proc.Procedures;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.tracing.TransactionTracer;
import org.neo4j.resources.CpuClock;
import org.neo4j.resources.HeapAllocation;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.test.Race;
import org.neo4j.time.Clocks;

import static java.lang.System.currentTimeMillis;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;
import static org.neo4j.internal.kernel.api.security.SecurityContext.AUTH_DISABLED;
import static org.neo4j.kernel.impl.util.collection.CollectionsFactorySupplier.ON_HEAP;

public class KernelTransactionTerminationTest
{
    private static final int TEST_RUN_TIME_MS = 5_000;

    @Test( timeout = TEST_RUN_TIME_MS * 20 )
    public void transactionCantBeTerminatedAfterItIsClosed() throws Throwable
    {
        runTwoThreads(
                tx -> tx.markForTermination( Status.Transaction.TransactionMarkedAsFailed ),
                tx ->
                {
                    close( tx );
                    assertFalse( tx.getReasonIfTerminated().isPresent() );
                    tx.initialize();
                }
        );
    }

    @Test( timeout = TEST_RUN_TIME_MS * 20 )
    public void closeTransaction() throws Throwable
    {
        BlockingQueue<Boolean> committerToTerminator = new LinkedBlockingQueue<>( 1 );
        BlockingQueue<TerminatorAction> terminatorToCommitter = new LinkedBlockingQueue<>( 1 );

        runTwoThreads(
                tx ->
                {
                    Boolean terminatorShouldAct = committerToTerminator.poll();
                    if ( terminatorShouldAct != null && terminatorShouldAct )
                    {
                        TerminatorAction action = TerminatorAction.random();
                        action.executeOn( tx );
                        assertTrue( terminatorToCommitter.add( action ) );
                    }
                },
                tx ->
                {
                    tx.initialize();
                    CommitterAction committerAction = CommitterAction.random();
                    committerAction.executeOn( tx );
                    if ( committerToTerminator.offer( true ) )
                    {
                        TerminatorAction terminatorAction;
                        try
                        {
                            terminatorAction = terminatorToCommitter.poll( 1, TimeUnit.SECONDS );
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

    private void runTwoThreads( Consumer<TestKernelTransaction> thread1Action,
            Consumer<TestKernelTransaction> thread2Action ) throws Throwable
    {
        TestKernelTransaction tx = TestKernelTransaction.create().initialize();
        AtomicLong t1Count = new AtomicLong();
        AtomicLong t2Count = new AtomicLong();
        long endTime = currentTimeMillis() + TEST_RUN_TIME_MS;
        int limit = 20_000;

        Race race = new Race();
        race.withEndCondition(
                () -> ((t1Count.get() >= limit) && (t2Count.get() >= limit)) || (currentTimeMillis() >= endTime) );
        race.addContestant( () ->
        {
            thread1Action.accept( tx );
            t1Count.incrementAndGet();
        } );
        race.addContestant( () ->
        {
            thread2Action.accept( tx );
            t2Count.incrementAndGet();
        } );
        race.go();
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
                    void executeOn( KernelTransaction tx )
                    {
                    }

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
                    void executeOn( KernelTransaction tx )
                    {
                        tx.success();
                    }

                    @Override
                    void closeTerminated( TestKernelTransaction tx )
                    {
                        tx.assertTerminated();
                        try
                        {
                            tx.close();
                            fail( "Exception expected" );
                        }
                        catch ( Exception e )
                        {
                            assertThat( e, instanceOf( TransactionTerminatedException.class ) );
                        }
                        tx.assertRolledBack();
                    }

                    @Override
                    void closeNotTerminated( TestKernelTransaction tx ) throws TransactionFailureException
                    {
                        tx.assertNotTerminated();
                        tx.close();
                        tx.assertCommitted();
                    }
                },
        MARK_FAILURE
                {
                    @Override
                    void executeOn( KernelTransaction tx )
                    {
                        tx.failure();
                    }

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
                },
        MARK_SUCCESS_AND_FAILURE
                {
                    @Override
                    void executeOn( KernelTransaction tx )
                    {
                        tx.success();
                        tx.failure();
                    }

                    @Override
                    void closeTerminated( TestKernelTransaction tx ) throws TransactionFailureException
                    {
                        MARK_SUCCESS.closeTerminated( tx );
                    }

                    @Override
                    void closeNotTerminated( TestKernelTransaction tx )
                    {
                        tx.assertNotTerminated();
                        try
                        {
                            tx.close();
                            fail( "Exception expected" );
                        }
                        catch ( Exception e )
                        {
                            assertThat( e, instanceOf( TransactionFailureException.class ) );
                        }
                        tx.assertRolledBack();
                    }
                };

        static final CommitterAction[] VALUES = values();

        abstract void executeOn( KernelTransaction tx );

        abstract void closeTerminated( TestKernelTransaction tx ) throws TransactionFailureException;

        abstract void closeNotTerminated( TestKernelTransaction tx ) throws TransactionFailureException;

        static CommitterAction random()
        {
            return VALUES[ThreadLocalRandom.current().nextInt( VALUES.length )];
        }
    }

    private static class  TestKernelTransaction extends KernelTransactionImplementation
    {
        final CommitTrackingMonitor monitor;

        TestKernelTransaction( CommitTrackingMonitor monitor )
        {
            super( mock( StatementOperationParts.class ), mock( SchemaWriteGuard.class ), new TransactionHooks(),
                    mock( ConstraintIndexCreator.class ), new Procedures(), TransactionHeaderInformationFactory.DEFAULT,
                    mock( TransactionCommitProcess.class ), monitor, () -> mock( ExplicitIndexTransactionState.class ),
                    mock( Pool.class ), Clocks.fakeClock(),
                    new AtomicReference<>( CpuClock.NOT_AVAILABLE ), new AtomicReference<>( HeapAllocation.NOT_AVAILABLE ),
                    TransactionTracer.NULL,
                    LockTracer.NONE, PageCursorTracerSupplier.NULL,
                    mock( StorageEngine.class, RETURNS_MOCKS ), new CanWrite(),
                    mock( DefaultCursors.class ), AutoIndexing.UNSUPPORTED, mock( ExplicitIndexStore.class ),
                    EmptyVersionContextSupplier.EMPTY, ON_HEAP, new StandardConstraintSemantics(), mock( SchemaState.class),
                    mock( IndexingService.class ), mock( IndexProviderMap.class ) );

            this.monitor = monitor;
        }

        static TestKernelTransaction create()
        {
            return new TestKernelTransaction( new CommitTrackingMonitor() );
        }

        TestKernelTransaction initialize()
        {
            initialize( 42, 42, new SimpleStatementLocks( new NoOpClient() ), Type.implicit, AUTH_DISABLED, 0L, 1L );
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
