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
package org.neo4j.kernel.impl.locking;


/**
 * Note: This is confusing to you. What is the difference between this and {@link Locks}? Well. {@link Locks} is the
 * primary locking component in neo. However, there are lower layers that use a separate locking mechanism (this),
 * which they can do under very strict rules of engagement.
 *
 * This implementation should be removed, and it's usage should be replaced by using the regular lock manager.
 *
 *
 *
 * An implementation of this interface must guarantee that locking is completely fair:
 * - Locks should be assigned in the order which they were claimed.
 * - If a write lock is waiting, new read locks may not be issued.
 *
 * It is acceptable for an implementation to limit the number of allowed concurrent read locks.
 *
 * Write locks must be exclusive. No more than one writer may lock the same resource at any given time, and no other
 * lock types may be issued while a write lock is held.
 *
 * The simples possible solution issues the same type of mutually exclusive locks for each lock type.
 *
 * @see AbstractLockService for implementation details.
 */
public interface LockService
{
    enum LockType
    {
        READ_LOCK,
        WRITE_LOCK
    }

    Lock acquireNodeLock( long nodeId, LockType type );

    Lock acquireRelationshipLock( long relationhipId, LockType type );

    public static final Lock NO_LOCK = new Lock()
    {
        @Override
        public void release()
        {
           // Nothing to release, I'm not a lock, mind you
        }
    };

    public static final LockService NO_LOCK_SERVICE = new LockService()
    {
        @Override
        public Lock acquireNodeLock( long nodeId, LockType type )
        {
            return NO_LOCK;
        }

        @Override
        public Lock acquireRelationshipLock( long relationhipId, LockType type )
        {
            return NO_LOCK;
        }
    };
}
