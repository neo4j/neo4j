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

import org.junit.Test;

import java.io.Flushable;
import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;

import org.neo4j.concurrent.BinaryLatch;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.io.pagecache.IOLimiter;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.TransactionAppender;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.impl.transaction.log.pruning.LogPruning;
import org.neo4j.kernel.impl.transaction.tracing.CheckPointTracer;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.internal.DatabaseHealth;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.StorageEngine;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
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
import static org.neo4j.test.ThreadTestUtils.forkFuture;

public class CheckPointerImplTest
{
    private static final SimpleTriggerInfo INFO = new SimpleTriggerInfo( "test" );

    private final TransactionIdStore txIdStore = mock( TransactionIdStore.class );
    private final CheckPointThreshold threshold = mock( CheckPointThreshold.class );
    private final StorageEngine storageEngine = mock( StorageEngine.class );
    private final LogPruning logPruning = mock( LogPruning.class );
    private final TransactionAppender appender = mock( TransactionAppender.class );
    private final DatabaseHealth health = mock( DatabaseHealth.class );
    private final CheckPointTracer tracer = mock( CheckPointTracer.class, RETURNS_MOCKS );
    private IOLimiter limiter = mock( IOLimiter.class );

    private final long initialTransactionId = 2L;
    private final long transactionId = 42L;
    private final LogPosition logPosition = new LogPosition( 16L, 233L );

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
        verifyZeroInteractions( storageEngine );
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
        verify( storageEngine, times( 1 ) ).flushAndForce( limiter );
        verify( health, times( 2 ) ).assertHealthy( IOException.class );
        verify( appender, times( 1 ) ).checkPoint( eq( logPosition ), any( LogCheckPointEvent.class ) );
        verify( threshold, times( 1 ) ).initialize( initialTransactionId );
        verify( threshold, times( 1 ) ).checkPointHappened( transactionId );
        verify( threshold, times( 1 ) ).isCheckPointingNeeded( transactionId, INFO );
        verify( logPruning, times( 1 ) ).pruneLogs( logPosition.getLogVersion() );
        verify( tracer, times( 1 ) ).beginCheckPoint();
        verifyNoMoreInteractions( storageEngine, health, appender, threshold, tracer );
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
        verify( storageEngine, times( 1 ) ).flushAndForce( limiter );
        verify( health, times( 2 ) ).assertHealthy( IOException.class );
        verify( appender, times( 1 ) ).checkPoint( eq( logPosition ), any( LogCheckPointEvent.class ) );
        verify( threshold, times( 1 ) ).initialize( initialTransactionId );
        verify( threshold, times( 1 ) ).checkPointHappened( transactionId );
        verify( threshold, never() ).isCheckPointingNeeded( transactionId, INFO );
        verify( logPruning, times( 1 ) ).pruneLogs( logPosition.getLogVersion() );
        verifyZeroInteractions( tracer );
        verifyNoMoreInteractions( storageEngine, health, appender, threshold, tracer );
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
        verify( storageEngine, times( 1 ) ).flushAndForce( limiter );
        verify( health, times( 2 ) ).assertHealthy( IOException.class );
        verify( appender, times( 1 ) ).checkPoint( eq( logPosition ), any( LogCheckPointEvent.class ) );
        verify( threshold, times( 1 ) ).initialize( initialTransactionId );
        verify( threshold, times( 1 ) ).checkPointHappened( transactionId );
        verify( threshold, never() ).isCheckPointingNeeded( transactionId, INFO );
        verify( logPruning, times( 1 ) ).pruneLogs( logPosition.getLogVersion() );
        verifyZeroInteractions( tracer );
        verifyNoMoreInteractions( storageEngine, health, appender, threshold, tracer );
    }

    @Test
    public void forceCheckPointShouldWaitTheCurrentCheckPointingToCompleteBeforeRunning() throws Throwable
    {
        // Given
        Lock lock = new ReentrantLock();
        final Lock spyLock = spy( lock );

        doAnswer( invocation ->
        {
            verify( appender ).checkPoint( any( LogPosition.class ), any( LogCheckPointEvent.class ) );
            reset( appender );
            invocation.callRealMethod();
            return null;
        } ).when( spyLock ).unlock();

        final CheckPointerImpl checkPointing = checkPointer( mutex( spyLock ) );
        mockTxIdStore();

        final CountDownLatch startSignal = new CountDownLatch( 2 );
        final CountDownLatch completed = new CountDownLatch( 2 );

        checkPointing.start();

        Thread checkPointerThread = new CheckPointerThread( checkPointing, startSignal, completed );

        Thread forceCheckPointThread = new Thread( () ->
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
        } );

        // when
        checkPointerThread.start();
        forceCheckPointThread.start();

        completed.await();

        verify( spyLock, times( 2 ) ).lock();
        verify( spyLock, times( 2 ) ).unlock();
    }

    private StoreCopyCheckPointMutex mutex( Lock lock )
    {
        return new StoreCopyCheckPointMutex( new ReadWriteLock()
        {
            @Override
            public Lock writeLock()
            {
                return lock;
            }

            @Override
            public Lock readLock()
            {
                throw new UnsupportedOperationException();
            }
        } );
    }

    @Test
    public void tryCheckPointShouldWaitTheCurrentCheckPointingToCompleteNoRunCheckPointButUseTheTxIdOfTheEarlierRun()
            throws Throwable
    {
        // Given
        Lock lock = mock( Lock.class );
        final CheckPointerImpl checkPointing = checkPointer( mutex( lock ) );
        mockTxIdStore();

        checkPointing.forceCheckPoint( INFO );

        verify( appender ).checkPoint( eq( logPosition ), any( LogCheckPointEvent.class ) );
        reset( appender );

        checkPointing.tryCheckPoint( INFO );

        verifyNoMoreInteractions( appender );
    }

    @Test
    public void mustUseIoLimiterFromFlushing() throws Throwable
    {
        limiter = ( stamp, ios, flushable ) -> 42;
        when( threshold.isCheckPointingNeeded( anyLong(), eq( INFO ) ) ).thenReturn( true, false );
        mockTxIdStore();
        CheckPointerImpl checkPointing = checkPointer();

        checkPointing.start();
        checkPointing.checkPointIfNeeded( INFO );

        verify( storageEngine ).flushAndForce( limiter );
    }

    @Test
    public void mustFlushAsFastAsPossibleDuringForceCheckPoint() throws Exception
    {
        AtomicBoolean doneDisablingLimits = new AtomicBoolean();
        limiter = new IOLimiter()
        {
            @Override
            public long maybeLimitIO( long previousStamp, int recentlyCompletedIOs, Flushable flushable )
            {
                return 0;
            }

            @Override
            public void enableLimit()
            {
                doneDisablingLimits.set( true );
            }
        };
        mockTxIdStore();
        CheckPointerImpl checkPointer = checkPointer();
        checkPointer.forceCheckPoint( new SimpleTriggerInfo( "test" ) );
        assertTrue( doneDisablingLimits.get() );
    }

    @Test
    public void mustFlushAsFastAsPossibleDuringTryCheckPoint() throws Exception
    {

        AtomicBoolean doneDisablingLimits = new AtomicBoolean();
        limiter = new IOLimiter()
        {
            @Override
            public long maybeLimitIO( long previousStamp, int recentlyCompletedIOs, Flushable flushable )
            {
                return 0;
            }

            @Override
            public void enableLimit()
            {
                doneDisablingLimits.set( true );
            }
        };
        mockTxIdStore();
        CheckPointerImpl checkPointer = checkPointer();
        checkPointer.tryCheckPoint( INFO );
        assertTrue( doneDisablingLimits.get() );
    }

    private void verifyAsyncActionCausesConcurrentFlushingRush(
            ThrowingConsumer<CheckPointerImpl,IOException> asyncAction ) throws Exception
    {
        AtomicLong limitDisableCounter = new AtomicLong();
        AtomicLong observedRushCount = new AtomicLong();
        BinaryLatch backgroundCheckPointStartedLatch = new BinaryLatch();
        BinaryLatch forceCheckPointStartLatch = new BinaryLatch();

        limiter = new IOLimiter()
        {
            @Override
            public long maybeLimitIO( long previousStamp, int recentlyCompletedIOs, Flushable flushable )
            {
                return 0;
            }

            @Override
            public void disableLimit()
            {
                limitDisableCounter.getAndIncrement();
                forceCheckPointStartLatch.release();
            }

            @Override
            public void enableLimit()
            {
                limitDisableCounter.getAndDecrement();
            }
        };

        mockTxIdStore();
        CheckPointerImpl checkPointer = checkPointer();

        doAnswer( invocation ->
        {
            backgroundCheckPointStartedLatch.release();
            forceCheckPointStartLatch.await();
            long newValue = limitDisableCounter.get();
            observedRushCount.set( newValue );
            return null;
        } ).when( storageEngine ).flushAndForce( limiter );

        Future<Object> forceCheckPointer = forkFuture( () ->
        {
            backgroundCheckPointStartedLatch.await();
            asyncAction.accept( checkPointer );
            return null;
        } );

        when( threshold.isCheckPointingNeeded( anyLong(), eq( INFO ) ) ).thenReturn( true );
        checkPointer.checkPointIfNeeded( INFO );
        forceCheckPointer.get();
        assertThat( observedRushCount.get(), is( 1L ) );
    }

    @Test( timeout = 5000 )
    public void mustRequestFastestPossibleFlushWhenForceCheckPointIsCalledDuringBackgroundCheckPoint() throws Exception
    {
        verifyAsyncActionCausesConcurrentFlushingRush(
                checkPointer -> checkPointer.forceCheckPoint( new SimpleTriggerInfo( "async" ) ) );
    }

    @Test( timeout = 5000 )
    public void mustRequestFastestPossibleFlushWhenTryCheckPointIsCalledDuringBackgroundCheckPoint() throws Exception
    {
        verifyAsyncActionCausesConcurrentFlushingRush(
                checkPointer -> checkPointer.tryCheckPoint( new SimpleTriggerInfo( "async" ) ) );
    }

    private CheckPointerImpl checkPointer( StoreCopyCheckPointMutex mutex )
    {
        return new CheckPointerImpl( txIdStore, threshold, storageEngine, logPruning, appender, health,
                NullLogProvider.getInstance(), tracer, limiter, mutex );
    }

    private CheckPointerImpl checkPointer()
    {
        return checkPointer( new StoreCopyCheckPointMutex() );
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
        private final CountDownLatch startSignal;
        private final CountDownLatch completed;

        CheckPointerThread( CheckPointerImpl checkPointing, CountDownLatch startSignal, CountDownLatch completed )
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
