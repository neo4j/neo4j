/**
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

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;

import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceWaitEvent;
import org.neo4j.kernel.impl.util.IdOrderingQueue;

/**
 * Forces transactions in batches, as opposed to per transaction.
 */
public class BatchingPhysicalTransactionAppender extends AbstractPhysicalTransactionAppender
{
    static class ThreadLink
    {
        final Thread thread;
        volatile ThreadLink next;
        volatile boolean done;

        public ThreadLink( Thread thread )
        {
            this.thread = thread;
        }

        public void unpark()
        {
            LockSupport.unpark( thread );
        }

        static final ThreadLink END = new ThreadLink( null );
        static {
            END.next = END;
        }
    }

    AtomicReference<ThreadLink> threadLinkHead = new AtomicReference<>( ThreadLink.END );

    private final Lock forceLock;

    public BatchingPhysicalTransactionAppender( LogFile logFile, LogRotation logRotation,
                                                TransactionMetadataCache transactionMetadataCache,
                                                TransactionIdStore transactionIdStore,
                                                IdOrderingQueue legacyIndexTransactionOrdering,
                                                KernelHealth kernelHealth )
    {
        super( logFile, logRotation, transactionMetadataCache, transactionIdStore,
                legacyIndexTransactionOrdering, kernelHealth );
        forceLock = new ReentrantLock();
    }

    /**
     * Called by the appender.
     */
    @Override
    protected void emptyBufferIntoChannel() throws IOException
    {   // The force thread will do it himself
    }

    /**
     * Called by the appender that just appended a transaction to the log.
     * @param logAppendEvent A trace event for the given log append operation.
     */
    @Override
    protected void forceAfterAppend( LogAppendEvent logAppendEvent ) throws IOException
    {
        // There's a benign race here, where we add our link before we update our next pointer.
        // This is okay, however, because unparkAll() spins when it sees a null next pointer.
        ThreadLink threadLink = new ThreadLink( Thread.currentThread() );
        threadLink.next = threadLinkHead.getAndSet( threadLink );
        int waitTicks = 127;

        try ( LogForceWaitEvent logForceWaitEvent = logAppendEvent.beginLogForceWait() )
        {
            do
            {
                if ( forceLock.tryLock() )
                {
                    try
                    {
                        forceLog( logAppendEvent );
                    }
                    finally
                    {
                        forceLock.unlock();

                        // We've released the lock, so unpark anyone who might have decided park while we were working.
                        // The most recently parked thread is the one most likely to still have warm caches, so that's
                        // the one we would prefer to unpark. Luckily, the stack nature of the ThreadLinks makes it easy
                        // to get to.
                        ThreadLink nextWaiter = threadLinkHead.get();
                        nextWaiter.unpark();
                    }
                }
                else
                {
                    waitTicks = waitForLogForce( waitTicks );
                }
            }
            while ( !threadLink.done );
        }
    }

    private void forceLog( LogAppendEvent logAppendEvent ) throws IOException
    {
        ThreadLink links = threadLinkHead.getAndSet( ThreadLink.END );

        try ( LogForceEvent logForceEvent = logAppendEvent.beginLogForce() )
        {
            force();
        }

        unparkAll( links );
    }

    private void unparkAll( ThreadLink links )
    {
        do
        {
            links.done = true;
            links.unpark();
            ThreadLink tmp;
            do
            {
                // Spin on this because of the racy update when consing.
                tmp = links.next;
            }
            while ( tmp == null );
            links = tmp;
        }
        while ( links != ThreadLink.END );
    }

    private int waitForLogForce( int waitTicks )
    {
        waitTicks &= 127;

        // We do this fancy spin to create CPU pipeline stalls in which other threads can run a few instructions.
        // The hope is that those other threads might make enough progress to either finish our work, or allow us
        // to continue.
        if ( ThreadLocalRandom.current().nextBoolean() )
        {
            return waitTicks - 1;
        }
        long parkTime = TimeUnit.MILLISECONDS.toNanos( 100 );
        LockSupport.parkNanos( this, parkTime );
        return waitTicks;
    }

    @Override
    public void force() throws IOException
    {
        // Empty buffer into channel. We want to synchronize with appenders somehow so that they
        // don't append while we're doing that. The way rotation is coordinated we can't synchronize
        // on logFile because it would cause deadlocks. Synchronizing on channel assumes that appenders
        // also synchronize on channel.
        synchronized ( channel )
        {
            channel.emptyBufferIntoChannelAndClearIt();
        }

        channel.force();
    }
}
