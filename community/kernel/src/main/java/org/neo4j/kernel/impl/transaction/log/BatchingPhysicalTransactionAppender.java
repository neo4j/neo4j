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
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.function.Factory;
import org.neo4j.kernel.KernelHealth;
import org.neo4j.kernel.impl.util.Counter;
import org.neo4j.kernel.impl.util.IdOrderingQueue;

import static org.neo4j.kernel.impl.util.NumberUtil.haveSameSign;

/**
 * Forces transactions in batches, as opposed to per transaction. There's a
 * {@link BatchingForceThread background thread} that does the actual forcing, where the committers merely waits
 * for that background thread to complete its round and increment a ticket they're waiting for.
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
    public static final ParkStrategy DEFAULT_WAIT_STRATEGY = new ParkStrategy.Park( 10 /*ms*/ );

    /**
     * Incremented for every call to {@link #append(org.neo4j.kernel.impl.transaction.TransactionRepresentation)}
     * and used by the appending thread to know when its transaction have been forced to disk.
     */
    private final Counter appenderTicket;

    static class ThreadLink
    {
        volatile ThreadLink next;
        Thread thread;

        public ThreadLink( Thread thread )
        {
            this.next = null;
            this.thread = thread;
        }

        static final ThreadLink END = new ThreadLink( null );
    }

    AtomicReference<ThreadLink> threadLinkHead = new AtomicReference<>( ThreadLink.END );

    /**
     * Set to the value of {@link #appenderTicket}, what that value was before starting a call to force the channel,
     * every time the channel has been forced, where calls to force are issued by the
     * {@link BatchingForceThread}. That thread keeps on going as long as {@link #appenderTicket} is ahead,
     * pauses a while if fully caught up.
     */
    private final Counter forceTicket;
    private boolean shutDown;
    private final BatchingForceThread forceThread;

    public BatchingPhysicalTransactionAppender( final LogFile logFile, final LogRotation logRotation,
            TransactionMetadataCache transactionMetadataCache, final TransactionIdStore transactionIdStore,
            IdOrderingQueue legacyIndexTransactionOrdering,
            Factory<Counter> counting,
            ParkStrategy idleBackoffStrategy,
            KernelHealth kernelHealth )
    {
        super( logFile, logRotation, transactionMetadataCache, transactionIdStore,
                legacyIndexTransactionOrdering, kernelHealth );
        appenderTicket = counting.newInstance();
        forceTicket = counting.newInstance();
        forceThread = new BatchingForceThread( new BatchingForceThread.Operation()
        {
            /**
             * Called by the forcing thread that forces now and then.
             */
            @Override
            public boolean perform() throws IOException
            {
                long currentAppenderTicket = appenderTicket.get();
                if ( forceTicket.get() == currentAppenderTicket )
                {
                    return false;
                }

                force();

                // Mark that we've forced at least the ticket we saw when waking up previously.
                // It's on the pessimistic side, but better safe than sorry.
                forceTicket.set( currentAppenderTicket );

                ThreadLink linkedOut = threadLinkHead.getAndSet( ThreadLink.END );

                while ( linkedOut != ThreadLink.END )
                {
                    LockSupport.unpark( linkedOut.thread );

                    while ( linkedOut.next == null )
                    {   // spin, waiting for appender thread to finish updating the chain
                    }

                    linkedOut = linkedOut.next;
                }

                return true;
            }
        }, idleBackoffStrategy );
        forceThread.start();
    }

    /**
     * Called by the appender.
     */
    @Override
    protected void emptyBufferIntoChannel() throws IOException
    {   // The force thread will do it himself
    }

    /**
     * Called by the appender.
     */
    @Override
    protected long getNextTicket()
    {
        return appenderTicket.incrementAndGet();
    }

    /**
     * Called by the appender that just appended a transaction to the log.
     */
    @Override
    protected void forceAfterAppend( long ticket ) throws IOException
    {
        ThreadLink threadLink = new ThreadLink( Thread.currentThread() );
        threadLink.next = threadLinkHead.getAndSet( threadLink );

        // Stay a while and listen... while:
        while (  // the forcer hasn't yet caught up with me
                 (ticket > forceTicket.get() ||
                 // OR I've wrapped around Long.MAX_VALUE
                 !haveSameSign( ticket, forceTicket.get() )) &&

                 // AND this appender hasn't yet been shut down
                 !shutDown &&
                 // AND the forcer is of good health
                 forceThread.checkHealth() )
        {
            LockSupport.unpark( forceThread );
            LockSupport.parkNanos( 100_000 ); // 0,1 ms
        }
    }

    @Override
    public void force() throws IOException
    {
        // Empty buffer into channel. We want to synchronize with appenders somehow so that they
        // don't append while we're doing that. The natural way is to synchronize on logFile,
        // which all other code around transaction appending does.
        synchronized ( logFile )
        {
            channel.emptyBufferIntoChannelAndClearIt();
        }

        // Now force the channel
        forceChannel();
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
