/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.storageengine.api;

/**
 * Mode of {@link StorageEngine#apply(CommandsToApply, TransactionApplicationMode) applying transactions}.
 * Depending on how transaction state have been built, additional work may need to be performed during
 * application of it.
 */
public enum TransactionApplicationMode
{
    /**
     * Transaction that is created in the "normal" way and has changed transaction state, which goes
     * to commit and produces commands from that. Such a transaction is able to alter cache since it has
     * all such high level information directly from the transaction state.
     */
    INTERNAL(
            false, // id tracking not needed since that is done in the transaction before commit
            false, // cache invalidation not needed since cache can be updated
            false  // no extra care in terms of idempotency needs to be taken
            ),

    /**
     * Transaction that comes from an external source and consists only of commands, i.e. it may not
     * contain enough information to f.ex. update cache, but applies to the store just like an internal
     * transaction does.
     */
    EXTERNAL(
            true, // id tracking needed since that hasn't been done prior to receiving this external transaction
            true, // cache invalidation needed since not enough information available to update cache
            false // no extra care in terms of idempotency needs to be taken
            ),

    /**
     * Transaction that is recovered, where commands are read, much like {@link #EXTERNAL}, but should
     * be applied differently where extra care should be taken to ensure idempotency. This is because
     * a recovered transaction may have already been applied previously to the store.
     */
    RECOVERY(
            true,  // id tracking not needed because id generators will be rebuilt after recovery anyway
            false, // during recovery there's not really a cache to invalidate so don't bother
            true   // extra care needs to be taken to ensure idempotency since this transaction
                   // may have been applied previously
            );

    private final boolean needsHighIdTracking;
    private final boolean needsCacheInvalidation;
    private final boolean needsIdempotencyChecks;

    TransactionApplicationMode( boolean needsHighIdTracking, boolean needsCacheInvalidation,
            boolean ensureIdempotency )
    {
        this.needsHighIdTracking = needsHighIdTracking;
        this.needsCacheInvalidation = needsCacheInvalidation;
        this.needsIdempotencyChecks = ensureIdempotency;
    }

    /**
     * @return whether or not applying a transaction need to track and update high ids of underlying stores.
     */
    public boolean needsHighIdTracking()
    {
        return needsHighIdTracking;
    }

    /**
     * @return whether or not applying a transaction need to do additional work of invalidating affected caches.
     */
    public boolean needsCacheInvalidationOnUpdates()
    {
        return needsCacheInvalidation;
    }

    /**
     * @return whether or not applying a transaction need to be extra cautious about idempotency.
     */
    public boolean needsIdempotencyChecks()
    {
        return needsIdempotencyChecks;
    }
}
