/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.kernel.impl.transaction.log.rotation.StoreFlusher;
import org.neo4j.kernel.impl.transaction.tracing.CheckPointTracer;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.DoubleLatch;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class CheckPointerImplTest
{

    private final TransactionIdStore txIdStore = mock( TransactionIdStore.class );
    private final CheckPointThreshold threshold = mock( CheckPointThreshold.class );
    private final StoreFlusher flusher = mock( StoreFlusher.class );
    private final LogPruning logPruning = mock( LogPruning.class );
    private final TransactionAppender appender = mock( TransactionAppender.class );
    private final KernelHealth health = mock( KernelHealth.class );
    private final CheckPointTracer tracer = mock( CheckPointTracer.class, RETURNS_MOCKS );

    private final long initialTransactionId = 2l;
    private final long transactionId = 42l;
    private final LogPosition logPosition = new LogPosition( 16l, 233l );



    @Test
    public void shouldNotFlushIfItIsNotNeeded() throws Throwable
    {
        // Given
        CheckPointerImpl checkPointing = checkPointer();
        when( threshold.isCheckPointingNeeded( anyLong() ) ).thenReturn( false );

        checkPointing.start();

        // When
        long txId = checkPointing.checkPointIfNeeded();

        // Then
        assertEquals( -1, txId );
        verifyZeroInteractions( flusher );
        verifyZeroInteractions( tracer );
        verifyZeroInteractions( appender );
    }

    @Test
    public void shouldFlushIfItIsNeeded() throws Throwable
    {
        // Given
        CheckPointerImpl checkPointing = checkPointer();
        when( threshold.isCheckPointingNeeded( anyLong() ) ).thenReturn( true, false );
        mockTxIdStore();

        checkPointing.start();

        // When
        long txId = checkPointing.checkPointIfNeeded();

        // Then
        assertEquals( transactionId, txId );
        verify( flusher, times( 1 ) ).forceEverything();
        verify( health, times( 2 ) ).assertHealthy( IOException.class );
        verify( appender, times( 1 ) ).checkPoint( eq( logPosition ), any( LogCheckPointEvent.class ) );
        verify( threshold, times( 1 ) ).initialize( initialTransactionId );
        verify( threshold, times( 1 ) ).checkPointHappened( transactionId );
        verify( threshold, times( 1 ) ).isCheckPointingNeeded( transactionId );
        verify( logPruning, times( 1 ) ).pruneLogs( logPosition.getLogVersion() );
        verify( tracer, times( 1 ) ).beginCheckPoint();
        verifyNoMoreInteractions( flusher, health, appender, threshold, tracer );
    }

    @Test
    public void shouldForceCheckPointAlways() throws Throwable
    {
        // Given
        CheckPointerImpl checkPointing = checkPointer();
        when( threshold.isCheckPointingNeeded( anyLong() ) ).thenReturn( false );
        mockTxIdStore();

        checkPointing.start();

        // When
        long txId = checkPointing.forceCheckPoint();

        // Then
        assertEquals( transactionId, txId );
        verify( flusher, times( 1 ) ).forceEverything();
        verify( health, times( 2 ) ).assertHealthy( IOException.class );
        verify( appender, times( 1 ) ).checkPoint( eq( logPosition ), any( LogCheckPointEvent.class ) );
        verify( threshold, times( 1 ) ).initialize( initialTransactionId );
        verify( threshold, times( 1 ) ).checkPointHappened( transactionId );
        verify( threshold, never() ).isCheckPointingNeeded( transactionId );
        verify( logPruning, times( 1 ) ).pruneLogs( logPosition.getLogVersion() );
        verifyZeroInteractions( tracer );
        verifyNoMoreInteractions( flusher, health, appender, threshold, tracer );
    }

    @Test
    public void shouldCheckPointAlwaysWhenThereIsNoRunningCheckPoint() throws Throwable
    {
        // Given
        CheckPointerImpl checkPointing = checkPointer();
        when( threshold.isCheckPointingNeeded( anyLong() ) ).thenReturn( false );
        mockTxIdStore();

        checkPointing.start();

        // When
        long txId = checkPointing.tryCheckPoint();

        // Then
        assertEquals( transactionId, txId );
        verify( flusher, times( 1 ) ).forceEverything();
        verify( health, times( 2 ) ).assertHealthy( IOException.class );
        verify( appender, times( 1 ) ).checkPoint( eq( logPosition ), any( LogCheckPointEvent.class ) );
        verify( threshold, times( 1 ) ).initialize( initialTransactionId );
        verify( threshold, times( 1 ) ).checkPointHappened( transactionId );
        verify( threshold, never() ).isCheckPointingNeeded( transactionId );
        verify( logPruning, times( 1 ) ).pruneLogs( logPosition.getLogVersion() );
        verifyZeroInteractions( tracer );
        verifyNoMoreInteractions( flusher, health, appender, threshold, tracer );
    }

    @Test
    public void forceCheckPointShouldWaitTheCurrentCheckPointingToCompleteBeforeRunning() throws Throwable
    {
        // Given
        final CheckPointerImpl checkPointing = checkPointer();
        when( threshold.isCheckPointingNeeded( anyLong() ) ).thenReturn( true, false );
        mockTxIdStore();

        final DoubleLatch checkPointIfNeededLatch = new DoubleLatch();
        final DoubleLatch forceCheckPointLatch = new DoubleLatch();
        final CountDownLatch verify = new CountDownLatch( 1 );

        final long forcedTransactionId = 65l;
        final LogPosition forceLogPosition = new LogPosition( 24l, 466l );

        LogPruningAnswer answer = new LogPruningAnswer( checkPointIfNeededLatch, forcedTransactionId,
                forceLogPosition, forceCheckPointLatch, verify );
        doAnswer( answer ).when( logPruning ).pruneLogs( logPosition.getLogVersion() );

        checkPointing.start();

        Thread checkPointerThread = new CheckPointerThread( checkPointing );

        final AtomicLong forcedTxId = new AtomicLong();
        Thread forceCheckPointThread = new Thread()
        {
            @Override
            public void run()
            {
                try
                {
                    forceCheckPointLatch.awaitStart();
                    forcedTxId.set( checkPointing.forceCheckPoint() );

                    forceCheckPointLatch.finish();
                }
                catch ( Throwable e )
                {
                    throw new RuntimeException( e );
                }
            }
        };

        // when
        checkPointerThread.start();
        forceCheckPointThread.start();

        checkPointIfNeededLatch.start();

        verify.await();
        verifyZeroInteractions( txIdStore, flusher, logPruning, appender, health );

        checkPointIfNeededLatch.finish();
        forceCheckPointLatch.awaitFinish();

        // Then
        assertEquals( forcedTransactionId, forcedTxId.get() );
        verify( flusher, times( 1 ) ).forceEverything();
        verify( health, times( 2 ) ).assertHealthy( IOException.class );
        verify( appender, times( 1 ) ).checkPoint( eq( forceLogPosition ), any( LogCheckPointEvent.class ) );
        verify( threshold, times( 1 ) ).checkPointHappened( forcedTransactionId );
        verify( threshold, never() ).isCheckPointingNeeded( forcedTransactionId );
        verify( logPruning, times( 1 ) ).pruneLogs( forceLogPosition.getLogVersion() );
        verifyNoMoreInteractions( flusher, health, appender, threshold );
    }

    @Test
    public void tryCheckPointShouldWaitTheCurrentCheckPointingToCompleteNoRunCheckPointButUseTheTxIdOfTheEarlierRun()
            throws Throwable
    {
        // Given
        final CheckPointerImpl checkPointing = checkPointer();
        when( threshold.isCheckPointingNeeded( anyLong() ) ).thenReturn( true, false );
        mockTxIdStore();

        final DoubleLatch checkPointIfNeededLatch = new DoubleLatch();
        final DoubleLatch tryCheckPointLatch = new DoubleLatch();
        final CountDownLatch verify = new CountDownLatch( 1 );

        final long notToBeUsedTransactionId = 65l;
        final LogPosition notToBeUsedLogPosition = new LogPosition( 24l, 466l );

        LogPruningAnswer answer = new LogPruningAnswer( checkPointIfNeededLatch, notToBeUsedTransactionId,
                notToBeUsedLogPosition, tryCheckPointLatch, verify );
        doAnswer( answer ).when( logPruning ).pruneLogs( logPosition.getLogVersion() );

        checkPointing.start();

        Thread checkPointerThread = new CheckPointerThread( checkPointing );

        final AtomicLong forcedTxId = new AtomicLong();
        Thread tryCheckPointerThread = new Thread()
        {
            @Override
            public void run()
            {
                try
                {
                    tryCheckPointLatch.awaitStart();
                    forcedTxId.set( checkPointing.tryCheckPoint() );

                    tryCheckPointLatch.finish();
                }
                catch ( Throwable e )
                {
                    throw new RuntimeException( e );
                }
            }
        };

        // when
        checkPointerThread.start();
        tryCheckPointerThread.start();

        checkPointIfNeededLatch.start();

        verify.await();
        verifyZeroInteractions( txIdStore, flusher, logPruning, appender, health );

        checkPointIfNeededLatch.finish();
        tryCheckPointLatch.awaitFinish();

        // Then
        assertEquals( transactionId, forcedTxId.get() );
        verifyZeroInteractions( flusher, health, threshold, logPruning );
    }

    private CheckPointerImpl checkPointer()
    {
        return new CheckPointerImpl( txIdStore, threshold, flusher, logPruning, appender, health,
                NullLogProvider.getInstance(), tracer );
    }

    private void mockTxIdStore()
    {
        long[] triggerCommittedTransaction = {transactionId, logPosition.getLogVersion(), logPosition.getByteOffset()};
        when( txIdStore.getLastClosedTransaction() ).thenReturn( triggerCommittedTransaction );
        when( txIdStore.getLastClosedTransactionId() ).thenReturn( initialTransactionId, transactionId, transactionId );
    }

    private void mockTxIdStore( long transactionId, LogPosition logPosition )
    {
        long[] triggerCommittedTransaction = {transactionId, logPosition.getLogVersion(), logPosition.getByteOffset()};
        when( txIdStore.getLastClosedTransaction() ).thenReturn( triggerCommittedTransaction );
        when( txIdStore.getLastClosedTransactionId() ).thenReturn( transactionId );
    }

    private static class CheckPointerThread extends Thread
    {
        private final CheckPointerImpl checkPointing;

        public CheckPointerThread( CheckPointerImpl checkPointing )
        {
            this.checkPointing = checkPointing;
        }

        @Override
        public void run()
        {
            try
            {
                checkPointing.checkPointIfNeeded();
            }
            catch ( IOException e )
            {
                throw new RuntimeException( e );
            }
        }
    }

    private class LogPruningAnswer implements Answer
    {
        private final DoubleLatch checkPointIfNeededLatch;
        private final long forcedTransactionId;
        private final LogPosition forceLogPosition;
        private final DoubleLatch forceCheckPointLatch;
        private final CountDownLatch verify;

        public LogPruningAnswer( DoubleLatch checkPointIfNeededLatch, long forcedTransactionId,
                LogPosition forceLogPosition, DoubleLatch forceCheckPointLatch, CountDownLatch verify )
        {
            this.checkPointIfNeededLatch = checkPointIfNeededLatch;
            this.forcedTransactionId = forcedTransactionId;
            this.forceLogPosition = forceLogPosition;
            this.forceCheckPointLatch = forceCheckPointLatch;
            this.verify = verify;
        }

        @Override
        public Void answer( InvocationOnMock invocation ) throws Throwable
        {
            checkPointIfNeededLatch.awaitStart();

            reset( txIdStore, threshold, flusher, logPruning, appender, health );
            mockTxIdStore( forcedTransactionId, forceLogPosition );

            forceCheckPointLatch.start();
            verify.countDown();
            checkPointIfNeededLatch.awaitFinish();

            return null;
        }
    }
}
