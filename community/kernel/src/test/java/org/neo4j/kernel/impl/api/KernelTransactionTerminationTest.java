/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.collection.pool.Pool;
import org.neo4j.function.Consumer;
import org.neo4j.graphdb.TransactionTerminatedException;
import org.neo4j.helpers.FakeClock;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.exceptions.TransactionFailureException;
import org.neo4j.kernel.api.labelscan.LabelScanStore;
import org.neo4j.kernel.api.txstate.LegacyIndexTransactionState;
import org.neo4j.kernel.impl.api.index.IndexingService;
import org.neo4j.kernel.impl.api.index.SchemaIndexProviderMap;
import org.neo4j.kernel.impl.api.state.ConstraintIndexCreator;
import org.neo4j.kernel.impl.api.store.ProcedureCache;
import org.neo4j.kernel.impl.api.store.StoreReadLayer;
import org.neo4j.kernel.impl.constraints.StandardConstraintSemantics;
import org.neo4j.kernel.impl.locking.NoOpLocks;
import org.neo4j.kernel.impl.locking.SimpleStatementLocksFactory;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.transaction.TransactionHeaderInformationFactory;
import org.neo4j.kernel.impl.transaction.TransactionMonitor;
import org.neo4j.kernel.impl.transaction.state.NeoStoreTransactionContext;
import org.neo4j.kernel.impl.transaction.state.TransactionRecordState;
import org.neo4j.kernel.impl.transaction.tracing.TransactionTracer;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.mock;

public class KernelTransactionTerminationTest
{
    private static final int TEST_RUN_TIME_MS = 5_000;

    @Test( timeout = TEST_RUN_TIME_MS * 2 )
    public void transactionCantBeTerminatedAfterItIsClosed() throws Exception
    {
        runTwoThreads( new Consumer<TestKernelTransaction>()
                       {
                           @Override
                           public void accept( TestKernelTransaction tx )
                           {
                               tx.markForTermination( Status.Transaction.MarkedAsFailed );
                           }
                       },
                new Consumer<TestKernelTransaction>()
                {
                    @Override
                    public void accept( TestKernelTransaction tx )
                    {
                        close( tx );
                        assertNull( tx.getReasonIfTerminated() );
                        tx.initialize();
                    }
                }
        );
    }

    @Test( timeout = TEST_RUN_TIME_MS * 2 )
    public void closeTransaction() throws Exception
    {
        final BlockingQueue<Boolean> committerToTerminator = new LinkedBlockingQueue<>( 1 );
        final BlockingQueue<TerminatorAction> terminatorToCommitter = new LinkedBlockingQueue<>( 1 );

        runTwoThreads( new Consumer<TestKernelTransaction>()
                       {
                           @Override
                           public void accept( TestKernelTransaction tx )
                           {
                               Boolean terminatorShouldAct = committerToTerminator.poll();
                               if ( terminatorShouldAct != null && terminatorShouldAct )
                               {
                                   TerminatorAction action = TerminatorAction.random();
                                   action.executeOn( tx );
                                   assertTrue( terminatorToCommitter.add( action ) );
                               }
                           }
                       },
                new Consumer<TestKernelTransaction>()
                {
                    @Override
                    public void accept( TestKernelTransaction tx )
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
                }
        );
    }

    private void runTwoThreads( final Consumer<TestKernelTransaction> thread1Action,
            final Consumer<TestKernelTransaction> thread2Action ) throws Exception
    {
        final TestKernelTransaction tx = TestKernelTransaction.create().initialize();

        final CountDownLatch start = new CountDownLatch( 1 );
        final AtomicBoolean stop = new AtomicBoolean();

        Future<?> action1 = Executors.newSingleThreadExecutor().submit( new Runnable()
        {
            @Override
            public void run()
            {
                await( start );
                while ( !stop.get() )
                {
                    thread1Action.accept( tx );
                }
            }
        } );

        Future<?> action2 = Executors.newSingleThreadExecutor().submit( new Runnable()
        {
            @Override
            public void run()
            {
                await( start );
                while ( !stop.get() )
                {
                    thread2Action.accept( tx );
                }
            }
        } );

        start.countDown();
        sleep();
        stop.set( true );

        assertNull( action1.get( 1, TimeUnit.MINUTES ) );
        assertNull( action2.get( 1, TimeUnit.MINUTES ) );
    }

    private static void await( CountDownLatch latch )
    {
        try
        {
            assertTrue( latch.await( 10, TimeUnit.SECONDS ) );
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
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

    private static void sleep() throws InterruptedException
    {
        Thread.sleep( TEST_RUN_TIME_MS );
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
                        tx.markForTermination( Status.Transaction.MarkedAsFailed );
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

    private static class TestKernelTransaction extends KernelTransactionImplementation
    {
        final CommitTrackingMonitor monitor;

        @SuppressWarnings( "unchecked" )
        TestKernelTransaction( CommitTrackingMonitor monitor )
        {
            super( mock( StatementOperationParts.class ),
                    mock( SchemaWriteGuard.class ), mock( LabelScanStore.class ), mock( IndexingService.class ),
                    mock( UpdateableSchemaState.class ), mock( TransactionRecordState.class ),
                    mock( SchemaIndexProviderMap.class ), mock( NeoStores.class, RETURNS_MOCKS ),
                    new TransactionHooks(),
                    mock( ConstraintIndexCreator.class ), TransactionHeaderInformationFactory.DEFAULT,
                    mock( TransactionCommitProcess.class ), monitor,
                    mock( StoreReadLayer.class, RETURNS_MOCKS ), mock( LegacyIndexTransactionState.class ),
                    mock( Pool.class ), new StandardConstraintSemantics(), new FakeClock(), TransactionTracer.NULL,
                    new ProcedureCache(), new SimpleStatementLocksFactory( new NoOpLocks() ),
                    mock( NeoStoreTransactionContext.class ), true );

            this.monitor = monitor;
        }

        static TestKernelTransaction create()
        {
            return new TestKernelTransaction( new CommitTrackingMonitor() );
        }

        TestKernelTransaction initialize()
        {
            initialize( 42, 42 );
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
            assertEquals( Status.Transaction.MarkedAsFailed, getReasonIfTerminated() );
            assertTrue( monitor.terminated );
        }

        void assertNotTerminated()
        {
            assertNull( getReasonIfTerminated() );
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
        public void transactionFinished( boolean successful )
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
        public void transactionTerminated()
        {
            terminated = true;
        }

        void reset()
        {
            committed = false;
            rolledBack = false;
            terminated = false;
        }
    }
}
