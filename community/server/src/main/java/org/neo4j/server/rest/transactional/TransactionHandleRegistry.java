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
package org.neo4j.server.rest.transactional;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.function.Predicate;
import org.neo4j.function.Predicates;
import org.neo4j.helpers.Clock;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.server.rest.transactional.error.InvalidConcurrentTransactionAccess;
import org.neo4j.server.rest.transactional.error.InvalidTransactionId;
import org.neo4j.server.rest.transactional.error.TransactionLifecycleException;

import static java.lang.String.format;

public class TransactionHandleRegistry implements TransactionRegistry
{
    private final AtomicLong idGenerator = new AtomicLong( 0l );
    private final ConcurrentHashMap<Long, TransactionMarker> registry = new ConcurrentHashMap<>( 64 );

    private final Clock clock;

    private final Log log;
    private final long timeoutMillis;

    public TransactionHandleRegistry( Clock clock, long timeoutMillis, LogProvider logProvider )
    {
        this.clock = clock;
        this.timeoutMillis = timeoutMillis;
        this.log = logProvider.getLog( getClass() );
    }

    private static abstract class TransactionMarker
    {
        abstract ActiveTransaction getActiveTransaction();

        abstract SuspendedTransaction getSuspendedTransaction() throws InvalidConcurrentTransactionAccess;

        abstract boolean isSuspended();
    }

    private static class ActiveTransaction extends TransactionMarker
    {
        final TransactionTerminationHandle terminationHandle;

        private ActiveTransaction( TransactionTerminationHandle terminationHandle )
        {
            this.terminationHandle = terminationHandle;
        }

        TransactionTerminationHandle getTerminationHandle()
        {
            return terminationHandle;
        }

        @Override
        ActiveTransaction getActiveTransaction()
        {
            return this;
        }

        @Override
        SuspendedTransaction getSuspendedTransaction() throws InvalidConcurrentTransactionAccess
        {
            throw new InvalidConcurrentTransactionAccess();
        }

        @Override
        boolean isSuspended()
        {
            return false;
        }
    }

    private class SuspendedTransaction extends TransactionMarker
    {
        final ActiveTransaction activeMarker;
        final TransactionHandle transactionHandle;
        final long lastActiveTimestamp;

        private SuspendedTransaction( ActiveTransaction activeMarker, TransactionHandle transactionHandle )
        {
            this.activeMarker = activeMarker;
            this.transactionHandle = transactionHandle;
            this.lastActiveTimestamp = clock.currentTimeMillis();
        }

        @Override
        ActiveTransaction getActiveTransaction()
        {
            return activeMarker;
        }

        @Override
        SuspendedTransaction getSuspendedTransaction() throws InvalidConcurrentTransactionAccess
        {
            return this;
        }

        @Override
        boolean isSuspended()
        {
            return true;
        }

        public long getLastActiveTimestamp()
        {
            return lastActiveTimestamp;
        }
    }

    @Override
    public long begin( TransactionHandle handle )
    {
        long id = idGenerator.incrementAndGet();
        if ( null == registry.putIfAbsent( id, new ActiveTransaction( handle ) ) )
        {
            return id;
        }
        else
        {
            throw new IllegalStateException( "Attempt to begin transaction for id that was already registered" );
        }
    }

    @Override
    public long release( long id, TransactionHandle transactionHandle )
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

        SuspendedTransaction suspendedTx = new SuspendedTransaction( marker.getActiveTransaction(), transactionHandle );
        if ( !registry.replace( id, marker, suspendedTx ) )
        {
            throw new IllegalStateException( "Trying to suspend transaction that has been concurrently suspended" );
        }
        return computeNewExpiryTime( suspendedTx.getLastActiveTimestamp() );
    }

    private long computeNewExpiryTime( long lastActiveTimestamp )
    {
        return  lastActiveTimestamp + timeoutMillis;
    }

    @Override
    public TransactionHandle acquire( long id ) throws TransactionLifecycleException
    {
        TransactionMarker marker = registry.get( id );

        if ( null == marker )
        {
            throw new InvalidTransactionId();
        }

        SuspendedTransaction transaction = marker.getSuspendedTransaction();
        if ( registry.replace( id, marker, marker.getActiveTransaction() ) )
        {
            return transaction.transactionHandle;
        }
        else
        {
            throw new InvalidConcurrentTransactionAccess();
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
    public TransactionHandle terminate( long id ) throws TransactionLifecycleException
    {
        TransactionMarker marker = registry.get( id );
        if ( null == marker )
        {
            throw new InvalidTransactionId();
        }
        else
        {
            TransactionTerminationHandle handle = marker.getActiveTransaction().getTerminationHandle();
            handle.terminate();

            try {
                return acquire( id );
            } catch (InvalidConcurrentTransactionAccess exception) {
                // We could not acquire the transaction. Let the other request clean up.
                return null;
            }
        }
    }

    @Override
    public void rollbackAllSuspendedTransactions()
    {
        rollbackSuspended( Predicates.<TransactionMarker>alwaysTrue() );
    }

    public void rollbackSuspendedTransactionsIdleSince( final long oldestLastActiveTime )
    {
        rollbackSuspended( new Predicate<TransactionMarker>()
        {
            @Override
            public boolean test( TransactionMarker item )
            {
                try
                {
                    SuspendedTransaction transaction = item.getSuspendedTransaction();
                    return transaction.lastActiveTimestamp < oldestLastActiveTime;
                }
                catch ( InvalidConcurrentTransactionAccess concurrentTransactionAccessError )
                {
                    throw new RuntimeException( concurrentTransactionAccessError );
                }
            }
        } );
    }

    private void rollbackSuspended( Predicate<TransactionMarker> predicate )
    {
        Set<Long> candidateTransactionIdsToRollback = new HashSet<Long>();

        for ( Map.Entry<Long, TransactionMarker> entry : registry.entrySet() )
        {
            TransactionMarker marker = entry.getValue();
            if (marker.isSuspended() && predicate.test(marker))
            {
                candidateTransactionIdsToRollback.add( entry.getKey() );
            }
        }

        for ( long id : candidateTransactionIdsToRollback )
        {
            TransactionHandle handle;
            try
            {
                handle = acquire( id );
            }
            catch ( TransactionLifecycleException invalidTransactionId )
            {
                // Allow this - someone snatched the transaction from under our feet,
                continue;
            }
            try
            {
                handle.forceRollback();
                log.info( format( "Transaction with id %d has been automatically rolled back.", id ) );
            }
            catch ( Throwable e )
            {
                log.error( format( "Transaction with id %d failed to roll back.", id ), e );
            }
            finally
            {
                forget( id );
            }
        }
    }
}
