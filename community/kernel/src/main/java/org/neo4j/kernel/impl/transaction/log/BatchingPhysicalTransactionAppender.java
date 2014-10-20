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
    public static final WaitStrategy DEFAULT_WAIT_STRATEGY = new WaitStrategy.Park( 10 /*ms*/ );

    /**
     * Incremented for every call to {@link #append(org.neo4j.kernel.impl.transaction.TransactionRepresentation)}
     * and used by the appending thread to know when its transaction have been forced to disk.
     */
    private final Counter appenderTicket;

    /**
     * Set to the value of {@link #appenderTicket}, what that value was before starting a call to force the channel,
     * every time the channel has been forced, where calls to force are issued by the
     * {@link BatchingForceThread}. That thread keeps on going as long as {@link #appenderTicket} is ahead,
     * pauses a while if fully caught up.
     */
    private final Counter forceTicket;
    private boolean shutDown;
    private final BatchingForceThread forceThread;

    public BatchingPhysicalTransactionAppender( LogFile logFile,
            TransactionMetadataCache transactionMetadataCache, final TransactionIdStore transactionIdStore,
            IdOrderingQueue legacyIndexTransactionOrdering,
            Factory<Counter> counting,
            WaitStrategy idleBackoffStrategy )
    {
        super( logFile, transactionMetadataCache, transactionIdStore, legacyIndexTransactionOrdering );
        appenderTicket = counting.newInstance();
        forceTicket = counting.newInstance();
        forceThread = new BatchingForceThread( new BatchingForceThread.Operation()
        {
            /**
             * Called by the forcing thread that forces now and then.
             */
            @Override
            public boolean force() throws IOException
            {
                long currentAppenderTicket = appenderTicket.get();
                if ( forceTicket.get() == currentAppenderTicket )
                {
                    return false;
                }

                forceChannel();
                forceTicket.set( currentAppenderTicket );
                return true;
            }
        }, idleBackoffStrategy );
        forceThread.start();
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
        LockSupport.unpark( forceThread );

        // Stay while...
        while ( // the forcer is of good health
                forceThread.checkHealth() &&
                // AND this appender hasn't yet been shut down
                !shutDown && // AND
                // EITHER the forcer has caught up with me
                (ticket > forceTicket.get() ||
                // OR I've wrapped around Long.MAX_VALUE
                !haveSameSign( ticket, forceTicket.get())) )
        {
            LockSupport.parkNanos( 10_000 ); // 10 us
        }
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
