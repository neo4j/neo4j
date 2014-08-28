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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.kernel.impl.nioneo.store.TransactionIdStore;

/**
 * Forces transactions in batches, as opposed to per transaction. There's a
 * {@link BatchingForceThread background thread} that does the actual forcing, where the committers merely waits
 * for that background thread to complete its round and increment a ticket they're waiting for.
 */
public class BatchingPhysicalTransactionAppender extends AbstractPhysicalTransactionAppender
{
    private final AtomicLong ongoingForceCounter = new AtomicLong( 0 );
    private final AtomicLong completedForceCounter = new AtomicLong( 0 );
    private boolean shutDown;
    private final BatchingForceThread forceThread;
    
    public BatchingPhysicalTransactionAppender( LogFile logFile, TxIdGenerator txIdGenerator,
            TransactionMetadataCache transactionMetadataCache, final TransactionIdStore transactionIdStore,
            IdOrderingQueue legacyIndexTransactionOrdering )
    {
        super( logFile, txIdGenerator, transactionMetadataCache, transactionIdStore, legacyIndexTransactionOrdering );
        forceThread = new BatchingForceThread( new BatchingForceThread.Operation()
        {
            private long lastSeenTransactionId;

            /**
             * Called by the forcing thread that forces now and then.
             */
            @Override
            public boolean force() throws IOException
            {
                long currentTransactionId = transactionIdStore.getLastCommittedTransactionId();
                ongoingForceCounter.incrementAndGet();
                if ( currentTransactionId != lastSeenTransactionId )
                {
                    synchronized ( channel )
                    {
                        channel.force();
                    }
                }
                completedForceCounter.incrementAndGet();
                
                boolean changed = lastSeenTransactionId != currentTransactionId;
                lastSeenTransactionId = currentTransactionId;
                return changed;
            }
        } );
        forceThread.start();
    }

    @Override
    protected long getCurrentTicket()
    {
        return ongoingForceCounter.get();
    }

    /**
     * Called by the committer that just appended a transaction to the log.
     */
    @Override
    protected void force( long ticket ) throws IOException
    {
        LockSupport.unpark( forceThread );
        while ( (ticket == ongoingForceCounter.get() || ticket == completedForceCounter.get()) &&
                !shutDown && forceThread.checkHealth() )
        {
            LockSupport.parkNanos( 100_000 ); // 0,1 ms
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
