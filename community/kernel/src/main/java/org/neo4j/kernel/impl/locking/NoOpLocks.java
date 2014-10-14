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
package org.neo4j.kernel.impl.locking;

public class NoOpLocks implements Locks
{
    @Override
    public void init() throws Throwable
    {
    }

    @Override
    public void start() throws Throwable
    {
    }

    @Override
    public void stop() throws Throwable
    {
    }

    @Override
    public void shutdown() throws Throwable
    {
    }

    @Override
    public Client newClient()
    {
        return null;
    }

    @Override
    public void accept( Visitor visitor )
    {
    }

    public static final Locks NO_LOCKS = new NoOpLocks();

    public static final Locks.Client NO_LOCKS_CLIENT = new Locks.Client()
    {
        @Override
        public void acquireShared( Locks.ResourceType resourceType, long... resourceIds ) throws AcquireLockTimeoutException
        {
        }

        @Override
        public void acquireExclusive( Locks.ResourceType resourceType, long... resourceIds ) throws AcquireLockTimeoutException
        {
        }

        @Override
        public boolean tryExclusiveLock( Locks.ResourceType resourceType, long... resourceIds )
        {
            return false;
        }

        @Override
        public boolean trySharedLock( Locks.ResourceType resourceType, long... resourceIds )
        {
            return false;
        }

        @Override
        public void releaseShared( Locks.ResourceType resourceType, long... resourceIds )
        {
        }

        @Override
        public void releaseExclusive( Locks.ResourceType resourceType, long... resourceIds )
        {
        }

        @Override
        public void releaseAllShared()
        {
        }

        @Override
        public void releaseAllExclusive()
        {
        }

        @Override
        public void releaseAll()
        {
        }

        @Override
        public void close()
        {
        }

        @Override
        public int getLockSessionId()
        {
            return 0;
        }
    };
}
