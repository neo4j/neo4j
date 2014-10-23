/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
import java.util.concurrent.locks.LockSupport;

import org.neo4j.function.Factory;
import org.neo4j.kernel.impl.util.Counter;
import org.neo4j.kernel.impl.util.IdOrderingQueue;

/**
 * Forces transactions in batches, as opposed to per transaction. There's a
 * {@link BatchingForceThread background thread} that does the actual forcing, where the committers merely wait
 * for that background thread to complete the force they're waiting for.
 */
public class BatchingPhysicalTransactionAppender extends AbstractPhysicalTransactionAppender
{
    /**
     * Default park duration is 10ms, the reason it's not lower is that a) unpark will have the forcer
     * thread wake up and continue straight away, no matter what. Plus on Windows, and potentially other systems,
     * there's an inconvenience where the operating system has a lowest granularity of 10ms, and
     * chooses to solve short pauses like this by temporarily changing its system-wide lowest granularity,
     * i.e. potentially affecting the rest of the operation system that this is run on.
     */
    public static final WaitStrategy DEFAULT_WAIT_STRATEGY = new WaitStrategy.Park( 10 /*ms*/ );

    public static final long LOOK_AHEAD_WAIT_TIME_NANOS = 10_000;
    public static final long MAX_FORCE_WAIT_TIME_MILLIS = 10;

    /* The force counter counts up twice for each actual force, representing a count up before and after
     * every force. This means that during an ongoing force the counter will be odd, and then when it completes
     * it ill become even again.
     *
     * If a thread waiting for the force sees an even number, then it must wait for the next even number since there
     * is no ongoing force. However, if it sees an odd number then there is an ongoing force and it is not sufficient
     * to wait for this ongoing force to finish, but rather the subsequent one.
     *
     * This is realized by waiting for either +2 or +3 of the current force counter.
     */
    private final Counter forceCount;

    /* The wait and pass counters are used to keep track of the ongoing workload.
     *
     * The difference (waitCount-passCount) equals the number of appending threads
     * waiting for a force to occur before they can proceed.
     */
    private final Counter waitCount;
    private final Counter passCount;

    private boolean shutDown;
    private final BatchingForceThread forceThread;

    volatile boolean forceThreadIdle = true;

    public BatchingPhysicalTransactionAppender( LogFile logFile,
            TransactionMetadataCache transactionMetadataCache, final TransactionIdStore transactionIdStore,
            IdOrderingQueue legacyIndexTransactionOrdering,
            Factory<Counter> counting,
            WaitStrategy idleBackoffStrategy )
    {
        super( logFile, transactionMetadataCache, transactionIdStore, legacyIndexTransactionOrdering );

        waitCount = counting.newInstance();
        passCount = counting.newInstance();

        forceCount = counting.newInstance();

        forceThread = new BatchingForceThread( new BatchingForceThread.Operation()
        {
            long lastForceMillis = 0;

            /**
             * Called by the forcing thread that forces now and then.
             */
            @Override
            public boolean force() throws IOException
            {
                if(waitCount.get() == passCount.get())
                {
                    forceThreadIdle = true;
                    return false; // Nothing to do at the moment.
                }

                forceThreadIdle = false;

                do
                {
                    long lastWaitCount = waitCount.get();
                    long lookAheadStart = System.nanoTime();

                    /* This is effectively a rate limiter on the single-threaded workload but is necessary
                     * as a look-ahead time for batching the forcing in a multi-threaded workload.
                     */
                    do
                    {
                        Thread.yield();
                    }
                    while(System.nanoTime() - lookAheadStart < LOOK_AHEAD_WAIT_TIME_NANOS);

                    /* Forcing is expensive and probably blocks all threads using the channel, thus in a multi-threaded
                     * load against the same channel its usage must be carefully managed. The strategy here is to wait
                     * until no more waiters trickle in during the look-ahead time or until 10 ms in total passes.
                     */
                    if ( lastWaitCount == waitCount.get() || (System.currentTimeMillis() - lastForceMillis) > MAX_FORCE_WAIT_TIME_MILLIS )
                    {
                        long fc = forceCount.get();

                        forceCount.set( fc + 1 );
                        forceChannel();
                        forceCount.set( fc + 2 );

                        lastForceMillis = System.currentTimeMillis();
                        break;
                    }
                } while ( true );

                return true;
            }
        }, idleBackoffStrategy );

        forceThread.start();
    }

    /**
     * Called by the appender that just appended a transaction to the log.
     */
    @Override
    protected void forceAfterAppend() throws IOException
    {
        waitCount.incrementAndGet();

        long forcePass = forceCount.get() + 2;
        if(forcePass % 2 != 0) forcePass++;

        while(forcePass - forceCount.get() > 0 )
        {
            if ( forceThreadIdle )
            {
                LockSupport.unpark( forceThread );
            }

            Thread.yield();
        }

        passCount.incrementAndGet();
    }

    @Override
    public void close()
    {
        forceThread.halt();
        try
        {
            forceThread.join();
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
        shutDown = true;
        super.close();
    }
}
