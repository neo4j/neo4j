/*
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
package org.neo4j.kernel.impl.locking;

import org.neo4j.helpers.Service;
import org.neo4j.kernel.impl.util.concurrent.WaitStrategy;
import org.neo4j.kernel.lifecycle.Lifecycle;

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
public interface Locks extends Lifecycle
{
    public abstract class Factory extends Service
    {
        public Factory( String key, String... altKeys )
        {
            super( key, altKeys );
        }

        public abstract Locks newInstance( ResourceType[] resourceTypes );
    }

    /** For introspection and debugging. */
    public interface Visitor
    {
        /** Visit the description of a lock held by at least one client. */
        void visit( ResourceType resourceType, long resourceId, String description, long estimatedWaitTime );
    }

    /** Locks are split by resource types. It is up to the implementation to define the contract for these. */
    public interface ResourceType
    {
        /** Must be unique among all existing resource types, should preferably be a sequence starting at 0. */
        int typeId();

        /** What to do if the lock cannot immediately be acquired. */
        WaitStrategy waitStrategy();
    }

    public interface Client extends AutoCloseable
    {
        /**
         * Can be grabbed when there are no locks or only share locks on a resource. If the lock cannot be acquired,
         * behavior is specified by the {@link WaitStrategy} for the given {@link ResourceType}.
         */
        void acquireShared(ResourceType resourceType, long ... resourceIds) throws AcquireLockTimeoutException;

        /**
         * Can be grabbed when no other client holds locks on the relevant resources. No other clients can hold locks
         * while one client holds an exclusive lock. If the lock cannot be acquired,
         * behavior is specified by the {@link WaitStrategy} for the given {@link ResourceType}.
         */
        void acquireExclusive(ResourceType resourceType, long ... resourceIds) throws AcquireLockTimeoutException;

        /** Try grabbing exclusive lock, not waiting and returning a boolean indicating if we got the lock. */
        boolean tryExclusiveLock( ResourceType resourceType, long ... resourceIds );

        /** Try grabbing shared lock, not waiting and returning a boolean indicating if we got the lock. */
        boolean trySharedLock( ResourceType resourceType, long ... resourceIds );

        /** Release a set of shared locks */
        void releaseShared( ResourceType resourceType, long... resourceIds );

        /** Release a set of exclusive locks */
        void releaseExclusive( ResourceType resourceType, long... resourceIds );

        /** Release all shared locks. */
        void releaseAllShared();

        /** Release all exclusive locks. */
        void releaseAllExclusive();

        /** Release all locks. */
        void releaseAll();

        /** Releases all locks, using the client after calling this is undefined. */
        @Override
        void close();

        /** For slave transactions, this tracks an identifier for the lock session running on the master */
        int getLockSessionId();
    }

    /**
     * A client is able to grab and release locks, and compete with other clients for them. This can be re-used until
     * you call {@link Locks.Client#close()}.
     */
    Client newClient();

    /** Visit all held locks. */
    void accept(Visitor visitor);
}
