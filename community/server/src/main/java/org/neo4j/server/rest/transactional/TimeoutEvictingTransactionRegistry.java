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

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.kernel.api.TransactionContext;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.server.rest.paging.Clock;
import org.neo4j.server.rest.transactional.error.InvalidTransactionIdError;
import org.neo4j.server.rest.transactional.error.Neo4jError;

public class TimeoutEvictingTransactionRegistry implements TransactionRegistry
{
    private final AtomicLong idGenerator = new AtomicLong( 0l );
    private final ConcurrentHashMap<Long, RegisteredTransaction> registry = new ConcurrentHashMap<Long, RegisteredTransaction>(50);

    private final Clock clock;
    private final StringLogger log;

    public TimeoutEvictingTransactionRegistry( Clock clock, StringLogger log )
    {
        this.clock = clock;
        this.log = log;
    }

    private class RegisteredTransaction
    {
        final TransitionalTxManagementTransactionContext ctx;
        final long storedAtTimestamp;

        private RegisteredTransaction( TransitionalTxManagementTransactionContext ctx, long currentTime )
        {
            this.ctx = ctx;
            this.storedAtTimestamp = currentTime;
        }
    }

    @Override
    public long newId() throws Neo4jError
    {
        return idGenerator.incrementAndGet();
    }

    /**
     * The only time a transaction is put into the registry is when either:
     *  * The user has asked to perform operations on it, in which case it will have been popped from the registry first
     *  * The user has asked for a new transaction, in which case we will have generated a guaranteed-to-be-unused id
     * Meaning:
     *    The architecture guarantees, and we depend on, the assumption that we will never be asked to put
     *    a transaction in here that already exists in the registry.
     */
    @Override
    public void put( long id, TransactionContext ctx ) throws Neo4jError
    {
        assert ctx instanceof TransitionalTxManagementTransactionContext :
                "During transition to kernel API, only TransitionalTxManagementTransactionContext are allowed.";

        TransitionalTxManagementTransactionContext castCtx = (TransitionalTxManagementTransactionContext) ctx;

        castCtx.suspendSinceTransactionsAreStillThreadBound();

        RegisteredTransaction item = new RegisteredTransaction( castCtx, clock.currentTimeInMilliseconds() );
        RegisteredTransaction preExisting = registry.putIfAbsent( id, item );

        assert preExisting == null : "Contract violation: " + id + " is already a registered transaction.";
    }

    @Override
    public TransactionContext pop( long id ) throws InvalidTransactionIdError
    {
        RegisteredTransaction item = registry.remove( id );
        if(item == null)
        {
            throw new InvalidTransactionIdError(
                    "The transaction you asked for cannot be found. " +
                    "Please ensure that you are not concurrently using the same transaction elsewhere. " +
                    "This could also be because the transaction has timed out and has been rolled back.", null);
        }
        item.ctx.resumeSinceTransactionsAreStillThreadBound();
        return item.ctx;
    }

    @Override
    public synchronized void evictAll()
    {
        for ( Long key : registry.keySet() )
        {
            try
            {
                pop( key ).rollback();
            }
            catch ( InvalidTransactionIdError neo4jError )
            {
                // Allow this - someone snatched the transaction from under our feet,
                // indicating someone is concurrently modifying transactions in the registry, which is allowed.
            }
        }
    }

    /**
     * Used to evict old transactions.
     */
    public void evictAllIdleSince( long maxAgeInMilliseconds )
    {
        for ( Map.Entry<Long, RegisteredTransaction> entry : registry.entrySet() )
        {
            long storedAtTimestamp = entry.getValue().storedAtTimestamp;
            if( storedAtTimestamp < maxAgeInMilliseconds )
            {
                try
                {
                    pop( entry.getKey() ).rollback();

                    long idleSeconds = MILLISECONDS.toSeconds( clock.currentTimeInMilliseconds() - storedAtTimestamp );
                    log.info( format( "Transaction with id %d has been idle for %d seconds, and has been " +
                            "automatically rolled back.", entry.getKey(), idleSeconds ) );
                }
                catch ( InvalidTransactionIdError neo4jError )
                {
                    // Allow this - someone snatched the transaction from under our feet,
                    // indicating someone is concurrently modifying transactions in the registry, which is allowed.
                }
            }
        }
    }
}
