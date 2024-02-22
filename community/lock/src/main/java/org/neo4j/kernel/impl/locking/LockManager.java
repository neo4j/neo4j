/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.locking;

import static org.neo4j.kernel.impl.locking.NoLocksClient.NO_LOCKS_CLIENT;

import org.neo4j.configuration.Config;
import org.neo4j.kernel.impl.api.LeaseClient;
import org.neo4j.lock.LockType;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.lock.ResourceType;
import org.neo4j.memory.MemoryTracker;

/**
 * API for managing locks.
 *
 * Locks are grabbed by clients (which generally map to a transaction, but can be any actor in the system).
 *
 * ## Upgrading and downgrading
 *
 * Shared locks allow upgrading, and exclusive locks allow downgrading. To upgrade a held shared lock to an exclusive
 * lock, simply acquire an exclusive lock and then release the shared lock. The acquire call will block other clients
 * from acquiring shared or exclusive locks, and then wait until all other holders of the shared locks have released
 * before returning.
 *
 * Downgrading a held exclusive lock is done by acquiring a shared lock, and then releasing the exclusive lock.
 *
 * ## Lock stacking
 *
 * Each call to acquire a lock must be accompanied by a call to release that same lock. A user can call acquire on the
 * same lock multiple times, thus requiring an equal number of calls to release those locks.
 */
public interface LockManager {
    /** For introspection and debugging. */
    interface Visitor {
        /** Visit the description of a lock held by at least one client. */
        void visit(
                LockType lockType,
                ResourceType resourceType,
                long transactionId,
                long resourceId,
                String description,
                long estimatedWaitTime,
                long lockIdentityHashCode);
    }

    interface Client extends ResourceLocker, AutoCloseable {
        /**
         * Invalid transaction id that lock clients using before they are initialised or after close
         */
        long INVALID_TRANSACTION_ID = -1;

        /**
         * Represents the fact that no lock session is used because no locks are taken.
         */
        int NO_LOCK_SESSION_ID = -1;

        /**
         * Initializes this locks client with a {@link LeaseClient} for the owning transaction. Must be called before any lock can be acquired.
         * An lease that has become invalid can abort a transaction midway.
         * @param leaseClient {@link LeaseClient} of the owning transaction.
         * @param transactionId lock client owning transaction id
         * @param memoryTracker memory tracker from the transaction
         * @param config neo4j configuration
         */
        void initialize(LeaseClient leaseClient, long transactionId, MemoryTracker memoryTracker, Config config);

        /** Try grabbing shared lock, not waiting and returning a boolean indicating if we got the lock. */
        boolean trySharedLock(ResourceType resourceType, long resourceId);

        /**
         * Start preparing this transaction for committing. In two-phase locking palace, we will in principle no longer
         * be acquiring any new locks - though we still allow it because it is useful in certain technical situations -
         * but when we are ready, we will start releasing them. This also means that we will no longer accept being
         * {@link #stop() asynchronously stopped}. From this point on, only the commit process can decide if the
         * transaction lives or dies, and in either case, the lock client will end up releasing all locks via the
         * {@link #close()} method.
         */
        void prepareForCommit();

        /**
         * Stop all active lock waiters and release them.
         * All new attempts to acquire any locks will cause exceptions.
         * This client can and should only be {@link #close() closed} afterwards.
         * If this client has been {@link #prepareForCommit() prepared}, then all currently acquired locks will remain held,
         * otherwise they will be released immediately.
         */
        void stop();

        /** Releases all locks, using the client after calling this is undefined. */
        @Override
        void close();

        long getTransactionId();

        long activeLockCount();

        void reset();
    }

    /**
     * A client is able to grab and release locks, and compete with other clients for them. This can be re-used until
     * you call {@link LockManager.Client#close()}.
     *
     * @throws IllegalStateException if this instance has been closed, i.e has had {@link #close()} called.
     */
    Client newClient();

    /** Visit all held locks. */
    void accept(Visitor visitor);

    void close();

    /** An implementation that doesn't do any locking **/
    LockManager NO_LOCKS_LOCK_MANAGER = new LockManager() {
        @Override
        public Client newClient() {
            return NO_LOCKS_CLIENT;
        }

        @Override
        public void accept(Visitor visitor) {}

        @Override
        public void close() {}
    };
}
