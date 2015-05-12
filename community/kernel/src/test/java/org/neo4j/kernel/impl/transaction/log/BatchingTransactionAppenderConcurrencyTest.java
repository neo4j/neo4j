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
package org.neo4j.kernel.impl.transaction.log;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.DeadSimpleTransactionIdStore;
import org.neo4j.kernel.impl.util.IdOrderingQueue;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.neo4j.test.ThreadTestUtils.awaitThreadState;
import static org.neo4j.test.ThreadTestUtils.fork;

public class BatchingTransactionAppenderConcurrencyTest
{
    private static enum ChannelCommand
    {
        emptyBufferIntoChannelAndClearIt,
        force,
        dummy
    }

    private static ExecutorService executor;

    @BeforeClass
    public static void setUpExecutor()
    {
        executor = Executors.newCachedThreadPool();
    }

    @AfterClass
    public static void tearDownExecutor()
    {
        executor.shutdown();
        executor = null;
    }

    private final LogAppendEvent logAppendEvent = LogAppendEvent.NULL;
    private LogFile logFile;
    private LogRotation logRotation;
    private TransactionMetadataCache transactionMetadataCache;
    private TransactionIdStore transactionIdStore;
    private IdOrderingQueue legacyindexTransactionOrdering;
    private KernelHealth kernelHealth;
    private WritableLogChannel channel;
    private BlockingQueue<ChannelCommand> channelCommandQueue;
    private Semaphore forceSemaphore;

    @Before
    public void setUp()
    {
        logFile = mock( LogFile.class );
        logRotation = LogRotation.NO_ROTATION;
        transactionMetadataCache = new TransactionMetadataCache( 10, 10 );
        transactionIdStore = new DeadSimpleTransactionIdStore();
        legacyindexTransactionOrdering = IdOrderingQueue.BYPASS;
        kernelHealth = mock( KernelHealth.class );
        channelCommandQueue = new LinkedBlockingQueue<>();
        forceSemaphore = new Semaphore( 0 );
        channel = new InMemoryLogChannel()
        {
            @Override
            public void force() throws IOException
            {
                try
                {
                    forceSemaphore.release();
                    channelCommandQueue.put( ChannelCommand.force );
                }
                catch ( InterruptedException e )
                {
                    throw new IOException( e );
                }
            }

            @Override
            public void emptyBufferIntoChannelAndClearIt()
            {
                try
                {
                    channelCommandQueue.put( ChannelCommand.emptyBufferIntoChannelAndClearIt );
                }
                catch ( InterruptedException e )
                {
                    throw new RuntimeException( e );
                }
            }
        };

        when( logFile.getWriter() ).thenReturn( channel );
    }

    private Runnable createForceAfterAppendRunnable( final BatchingTransactionAppender appender )
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    appender.forceAfterAppend( logAppendEvent );
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }
        };
    }

    @Test
    public void shouldForceLogChannel() throws Exception
    {
        BatchingTransactionAppender appender = new BatchingTransactionAppender( logFile, logRotation,
                transactionMetadataCache, transactionIdStore, legacyindexTransactionOrdering, kernelHealth );

        appender.forceAfterAppend( logAppendEvent );

        assertThat( channelCommandQueue.take(), is( ChannelCommand.emptyBufferIntoChannelAndClearIt ) );
        assertThat( channelCommandQueue.take(), is( ChannelCommand.force ) );
        assertTrue( channelCommandQueue.isEmpty() );
    }

    @Test
    public void shouldWaitForOngoingForceToCompleteBeforeForcingAgain() throws Exception
    {
        channelCommandQueue = new LinkedBlockingQueue<>( 2 );
        channelCommandQueue.put( ChannelCommand.dummy );

        // The 'emptyBuffer...' command will be put into the queue, and then it'll block on 'force' because the queue
        // will be at capacity.

        final BatchingTransactionAppender appender = new BatchingTransactionAppender( logFile, logRotation,
                transactionMetadataCache, transactionIdStore, legacyindexTransactionOrdering, kernelHealth );

        Runnable runnable = createForceAfterAppendRunnable( appender );
        Future<?> future = executor.submit( runnable );

        forceSemaphore.acquire();

        Thread otherThread = fork( runnable );
        awaitThreadState( otherThread, 5000, Thread.State.TIMED_WAITING );

        assertThat( channelCommandQueue.take(), is( ChannelCommand.dummy ) );
        assertThat( channelCommandQueue.take(), is( ChannelCommand.emptyBufferIntoChannelAndClearIt ) );
        assertThat( channelCommandQueue.take(), is( ChannelCommand.force ) );
        assertThat( channelCommandQueue.take(), is( ChannelCommand.emptyBufferIntoChannelAndClearIt ) );
        assertThat( channelCommandQueue.take(), is( ChannelCommand.force ) );
        future.get();
        otherThread.join();
        assertTrue( channelCommandQueue.isEmpty() );
    }

    @Test
    public void shouldBatchUpMultipleWaitingForceRequests() throws Exception
    {
        channelCommandQueue = new LinkedBlockingQueue<>( 2 );
        channelCommandQueue.put( ChannelCommand.dummy );

        // The 'emptyBuffer...' command will be put into the queue, and then it'll block on 'force' because the queue
        // will be at capacity.

        final BatchingTransactionAppender appender = new BatchingTransactionAppender( logFile, logRotation,
                transactionMetadataCache, transactionIdStore, legacyindexTransactionOrdering, kernelHealth );

        Runnable runnable = createForceAfterAppendRunnable( appender );
        Future<?> future = executor.submit( runnable );

        forceSemaphore.acquire();

        Thread[] otherThreads = new Thread[10];
        for ( int i = 0; i < otherThreads.length; i++ )
        {
            otherThreads[i] = fork( runnable );
        }
        for ( Thread otherThread : otherThreads )
        {
            awaitThreadState( otherThread, 5000, Thread.State.TIMED_WAITING );
        }

        assertThat( channelCommandQueue.take(), is( ChannelCommand.dummy ) );
        assertThat( channelCommandQueue.take(), is( ChannelCommand.emptyBufferIntoChannelAndClearIt ) );
        assertThat( channelCommandQueue.take(), is( ChannelCommand.force ) );
        assertThat( channelCommandQueue.take(), is( ChannelCommand.emptyBufferIntoChannelAndClearIt ) );
        assertThat( channelCommandQueue.take(), is( ChannelCommand.force ) );
        future.get();
        for ( Thread otherThread : otherThreads )
        {
            otherThread.join();
        }
        assertTrue( channelCommandQueue.isEmpty() );
    }
}
