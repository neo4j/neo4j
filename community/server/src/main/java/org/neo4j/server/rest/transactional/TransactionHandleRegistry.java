/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.server.rest.transactional;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.Predicates;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.server.rest.paging.Clock;
import org.neo4j.server.rest.transactional.error.InvalidConcurrentConcurrentTransactionAccess;
import org.neo4j.server.rest.transactional.error.InvalidTransactionId;

public class TransactionHandleRegistry implements TransactionRegistry
{
    private final AtomicLong idGenerator = new AtomicLong( 0l );
    private final ConcurrentHashMap<Long, TransactionMarker> registry =
            new ConcurrentHashMap<Long, TransactionMarker>( 64 );

    private final Clock clock;
    private final StringLogger log;

    public TransactionHandleRegistry( Clock clock, StringLogger log )
    {
        this.clock = clock;
        this.log = log;
    }

    private static abstract class TransactionMarker
    {
        abstract SuspendedTransaction getTransaction() throws InvalidConcurrentConcurrentTransactionAccess;

        abstract boolean isSuspended();
    }

    private static class ActiveTransaction extends TransactionMarker
    {
        public static final ActiveTransaction INSTANCE = new ActiveTransaction();

        @Override
        SuspendedTransaction getTransaction() throws InvalidConcurrentConcurrentTransactionAccess
        {
            throw new InvalidConcurrentConcurrentTransactionAccess();
        }

        boolean isSuspended()
        {
            return false;
        }
    }

    private class SuspendedTransaction extends TransactionMarker
    {
        final TransactionHandle transactionHandle;
        final long lastActiveTimestamp;

        private SuspendedTransaction( TransactionHandle transactionHandle )
        {
            this.transactionHandle = transactionHandle;
            this.lastActiveTimestamp = clock.currentTimeInMilliseconds();
        }

        @Override
        SuspendedTransaction getTransaction() throws InvalidConcurrentConcurrentTransactionAccess
        {
            return this;
        }

        boolean isSuspended()
        {
            return true;
        }
    }

    @Override
    public long begin()
    {
        long id = idGenerator.incrementAndGet();
        if ( null == registry.putIfAbsent( id, ActiveTransaction.INSTANCE ) )
        {
            return id;
        }
        else
        {
            throw new IllegalStateException( "Attempt to begin transaction for id that was already registered" );
        }
    }

    @Override
    public void release( long id, TransactionHandle transactionHandle )
    {
        TransactionMarker marker = registry.get( id );

        if ( null == marker )
        {
            throw new IllegalStateException( "Trying to suspend unregistered transaction" );
        }

        if ( marker.isSuspended() )
        {
            throw new IllegalStateException( "Trying to suspend transaction that was already suspended" );
        }

        SuspendedTransaction transaction = new SuspendedTransaction( transactionHandle );
        if ( !registry.replace( id, marker, transaction ) )
        {
            throw new IllegalStateException( "Trying to suspend transaction that has been concurrently suspended" );
        }
    }

    @Override
    public TransactionHandle acquire( long id ) throws InvalidTransactionId, InvalidConcurrentConcurrentTransactionAccess
    {
        TransactionMarker marker = registry.get( id );

        if ( null == marker )
        {
            throw new InvalidTransactionId();
        }

        if ( !marker.isSuspended() )
        {
            throw new InvalidConcurrentConcurrentTransactionAccess();
        }

        SuspendedTransaction transaction = marker.getTransaction();
        if ( registry.replace( id, marker, ActiveTransaction.INSTANCE ) )
        {
            return transaction.transactionHandle;
        }
        else
        {
            throw new InvalidConcurrentConcurrentTransactionAccess();
        }
    }

    @Override
    public void forget( long id )
    {
        TransactionMarker marker = registry.get( id );

        if ( null == marker )
        {
            throw new IllegalStateException( "Could not finish unregistered transaction" );
        }

        if ( marker.isSuspended() )
        {
            throw new IllegalStateException( "Cannot finish suspended registered transaction" );
        }

        if ( !registry.remove( id, marker ) )
        {
            throw new IllegalStateException(
                    "Trying to finish transaction that has been concurrently finished or suspended" );
        }
    }

    @Override
    public void rollbackAllSuspendedTransactions()
    {
        rollbackSuspended( Predicates.<TransactionMarker>TRUE() );
    }

    public void rollbackSuspendedTransactionsIdleSince( final long oldestLastActiveTime )
    {
        rollbackSuspended( new Predicate<TransactionMarker>()
        {
            @Override
            public boolean accept( TransactionMarker item )
            {
                try
                {
                    return item.getTransaction().lastActiveTimestamp < oldestLastActiveTime;
                }
                catch ( InvalidConcurrentConcurrentTransactionAccess concurrentTransactionAccessError )
                {
                    throw new RuntimeException( concurrentTransactionAccessError );
                }
            }
        } );

    }

    private void rollbackSuspended( Predicate<TransactionMarker> predicate )
    {
        Iterator<Map.Entry<Long,TransactionMarker>> entries = registry.entrySet().iterator();
        while ( entries.hasNext() )
        {
            Map.Entry<Long, TransactionMarker> entry = entries.next();
            TransactionMarker marker = entry.getValue();
            try
            {
                if ( predicate.accept( marker ) && marker.isSuspended() )
                {
                    TransactionHandle transactionHandle = marker.getTransaction().transactionHandle;
                    transactionHandle.forceRollback();
                    entries.remove();

                    long idleSeconds = MILLISECONDS.toSeconds( clock.currentTimeInMilliseconds() -
                            marker.getTransaction().lastActiveTimestamp );
                    log.info( format( "Transaction with id %d has been idle for %d seconds, and has been " +
                            "automatically rolled back.", entry.getKey(), idleSeconds ) );
                }
            }
            catch ( InvalidConcurrentConcurrentTransactionAccess concurrentTransactionAccessError )
            {
                // Allow this - someone snatched the transaction from under our feet,
                // indicating someone is concurrently modifying transactions in the registry, which is allowed.
            }
        }
    }

}
