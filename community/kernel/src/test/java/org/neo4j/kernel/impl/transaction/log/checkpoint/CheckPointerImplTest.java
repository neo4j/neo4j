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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.kernel.impl.transaction.log.rotation.StoreFlusher;
import org.neo4j.kernel.impl.transaction.tracing.CheckPointTracer;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.RETURNS_MOCKS;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.reset;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class CheckPointerImplTest
{
    private static final SimpleTriggerInfo INFO = new SimpleTriggerInfo( "test" );

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
        when( threshold.isCheckPointingNeeded( anyLong(), any( TriggerInfo.class ) ) ).thenReturn( false );

        checkPointing.start();

        // When
        long txId = checkPointing.checkPointIfNeeded( INFO );

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
        when( threshold.isCheckPointingNeeded( anyLong(), eq( INFO ) ) ).thenReturn( true, false );
        mockTxIdStore();

        checkPointing.start();

        // When
        long txId = checkPointing.checkPointIfNeeded( INFO );

        // Then
        assertEquals( transactionId, txId );
        verify( flusher, times( 1 ) ).forceEverything();
        verify( health, times( 2 ) ).assertHealthy( IOException.class );
        verify( appender, times( 1 ) ).checkPoint( eq( logPosition ), any( LogCheckPointEvent.class ) );
        verify( threshold, times( 1 ) ).initialize( initialTransactionId );
        verify( threshold, times( 1 ) ).checkPointHappened( transactionId );
        verify( threshold, times( 1 ) ).isCheckPointingNeeded( transactionId, INFO );
        verify( logPruning, times( 1 ) ).pruneLogs( logPosition.getLogVersion() );
        verify( tracer, times( 1 ) ).beginCheckPoint();
        verifyNoMoreInteractions( flusher, health, appender, threshold, tracer );
    }

    @Test
    public void shouldForceCheckPointAlways() throws Throwable
    {
        // Given
        CheckPointerImpl checkPointing = checkPointer();
        when( threshold.isCheckPointingNeeded( anyLong(), eq( INFO ) ) ).thenReturn( false );
        mockTxIdStore();

        checkPointing.start();

        // When
        long txId = checkPointing.forceCheckPoint( INFO );

        // Then
        assertEquals( transactionId, txId );
        verify( flusher, times( 1 ) ).forceEverything();
        verify( health, times( 2 ) ).assertHealthy( IOException.class );
        verify( appender, times( 1 ) ).checkPoint( eq( logPosition ), any( LogCheckPointEvent.class ) );
        verify( threshold, times( 1 ) ).initialize( initialTransactionId );
        verify( threshold, times( 1 ) ).checkPointHappened( transactionId );
        verify( threshold, never() ).isCheckPointingNeeded( transactionId, INFO );
        verify( logPruning, times( 1 ) ).pruneLogs( logPosition.getLogVersion() );
        verifyZeroInteractions( tracer );
        verifyNoMoreInteractions( flusher, health, appender, threshold, tracer );
    }

    @Test
    public void shouldCheckPointAlwaysWhenThereIsNoRunningCheckPoint() throws Throwable
    {
        // Given
        CheckPointerImpl checkPointing = checkPointer();
        when( threshold.isCheckPointingNeeded( anyLong(), eq( INFO ) ) ).thenReturn( false );
        mockTxIdStore();

        checkPointing.start();

        // When
        long txId = checkPointing.tryCheckPoint( INFO );

        // Then
        assertEquals( transactionId, txId );
        verify( flusher, times( 1 ) ).forceEverything();
        verify( health, times( 2 ) ).assertHealthy( IOException.class );
        verify( appender, times( 1 ) ).checkPoint( eq( logPosition ), any( LogCheckPointEvent.class ) );
        verify( threshold, times( 1 ) ).initialize( initialTransactionId );
        verify( threshold, times( 1 ) ).checkPointHappened( transactionId );
        verify( threshold, never() ).isCheckPointingNeeded( transactionId, INFO );
        verify( logPruning, times( 1 ) ).pruneLogs( logPosition.getLogVersion() );
        verifyZeroInteractions( tracer );
        verifyNoMoreInteractions( flusher, health, appender, threshold, tracer );
    }

    @Test
    public void forceCheckPointShouldWaitTheCurrentCheckPointingToCompleteBeforeRunning() throws Throwable
    {
        // Given
        ReentrantLock reentrantLock = new ReentrantLock();
        final Lock spyLock = spy( reentrantLock );

        doAnswer( new Answer()
        {
            @Override
            public Object answer( InvocationOnMock invocation ) throws Throwable
            {
                verify( appender ).checkPoint( any( LogPosition.class ), any( LogCheckPointEvent.class ) );
                reset( appender );
                invocation.callRealMethod();
                return null;
            }
        } ).when( spyLock ).unlock();

        final CheckPointerImpl checkPointing = checkPointer( spyLock );
        mockTxIdStore();

        final CountDownLatch startSignal = new CountDownLatch( 2 );
        final CountDownLatch completed = new CountDownLatch( 2 );

        checkPointing.start();

        Thread checkPointerThread = new CheckPointerThread( checkPointing, startSignal, completed );

        Thread forceCheckPointThread = new Thread()
        {
            @Override
            public void run()
            {
                try
                {
                    startSignal.countDown();
                    startSignal.await();
                    checkPointing.forceCheckPoint( INFO );

                    completed.countDown();
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

        completed.await();

        verify( spyLock, times( 2 ) ).lock();
        verify( spyLock, times( 2 ) ).unlock();
    }

    @Test
    public void tryCheckPointShouldWaitTheCurrentCheckPointingToCompleteNoRunCheckPointButUseTheTxIdOfTheEarlierRun()
            throws Throwable
    {
        // Given
        Lock lock = mock( Lock.class );
        final CheckPointerImpl checkPointing = checkPointer( lock );
        mockTxIdStore();

        checkPointing.forceCheckPoint( INFO );

        verify( appender ).checkPoint( eq( logPosition ), any( LogCheckPointEvent.class ) );
        reset( appender );

        checkPointing.tryCheckPoint( INFO );

        verifyNoMoreInteractions( appender );
    }

    private CheckPointerImpl checkPointer( Lock lock )
    {
        return new CheckPointerImpl( txIdStore, threshold, flusher, logPruning, appender, health,
                NullLogProvider.getInstance(), tracer, lock );
    }

    private CheckPointerImpl checkPointer()
    {
        return checkPointer( new ReentrantLock() );
    }

    private void mockTxIdStore()
    {
        long[] triggerCommittedTransaction = {transactionId, logPosition.getLogVersion(), logPosition.getByteOffset()};
        when( txIdStore.getLastClosedTransaction() ).thenReturn( triggerCommittedTransaction );
        when( txIdStore.getLastClosedTransactionId() ).thenReturn( initialTransactionId, transactionId, transactionId );
    }

    private static class CheckPointerThread extends Thread
    {
        private final CheckPointerImpl checkPointing;
        private CountDownLatch startSignal;
        private CountDownLatch completed;

        public CheckPointerThread( CheckPointerImpl checkPointing, CountDownLatch startSignal,
                CountDownLatch completed )
        {
            this.checkPointing = checkPointing;
            this.startSignal = startSignal;
            this.completed = completed;
        }

        @Override
        public void run()
        {
            try
            {
                startSignal.countDown();
                startSignal.await();
                checkPointing.forceCheckPoint( INFO );
                completed.countDown();
            }
            catch ( Exception e )
            {
                throw new RuntimeException( e );
            }
        }
    }

}
