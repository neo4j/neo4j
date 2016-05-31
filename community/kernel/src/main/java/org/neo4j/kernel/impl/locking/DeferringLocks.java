/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import org.neo4j.kernel.lifecycle.Lifecycle;

public class DeferringLocks extends Lifecycle.Delegate implements Locks
{
    private final Locks delegate;

    public DeferringLocks( Locks delegate )
    {
        super( delegate );
        this.delegate = delegate;
    }

    @Override
    public Client newClient()
    {
        return new DeferringLockClient( delegate.newClient() );
    }

    @Override
    public void accept( Visitor visitor )
    {
        delegate.accept( visitor );
    }

    static class Resource implements Comparable<Resource>
    {
        final ResourceType resourceType;
        final long resourceId;
        final boolean exclusive;

        Resource( ResourceType resourceType, long resourceId, boolean exclusive )
        {
            this.resourceType = resourceType;
            this.resourceId = resourceId;
            this.exclusive = exclusive;
        }

        @Override
        public int hashCode()
        {
            final int prime = 31;
            int result = 1;
            result = prime * result + (exclusive ? 1231 : 1237);
            result = prime * result + (int) (resourceId ^ (resourceId >>> 32));
            result = prime * result + resourceType.hashCode();
            return result;
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( this == obj )
                return true;
            if ( obj == null )
                return false;
            if ( getClass() != obj.getClass() )
                return false;
            Resource other = (Resource) obj;
            if ( exclusive != other.exclusive )
                return false;
            if ( resourceId != other.resourceId )
                return false;
            else if ( resourceType.typeId() != other.resourceType.typeId() )
                return false;
            return true;
        }

        @Override
        public int compareTo( Resource o )
        {
            // The important thing isn't the order itself, it's the presence of an order
            // so that all lock clients gets the same order
            if ( exclusive != o.exclusive )
            {
                return intOf( exclusive ) - intOf( o.exclusive );
            }

            return resourceType.typeId() == o.resourceType.typeId() ? Long.compare( resourceId, o.resourceId )
                                                                    : resourceType.typeId() - o.resourceType.typeId();
        }

        @Override
        public String toString()
        {
            return "Resource [resourceType=" + resourceType + ", resourceId=" + resourceId + ", exclusive=" + exclusive
                    + "]";
        }

        private static int intOf( boolean value )
        {
            return value ? 1 : 0;
        }
    }

    // TODO the state keeping in this class is quite unoptimized, please do so
    private static class DeferringLockClient implements Client
    {
        private final Client clientDelegate;
        private final Set<Resource> locks = new TreeSet<>();
        private boolean shouldStop;

        public DeferringLockClient( Client clientDelegate )
        {
            this.clientDelegate = clientDelegate;
        }

        @Override
        public void acquireShared( ResourceType resourceType, long... resourceIds ) throws AcquireLockTimeoutException
        {
            for ( long resourceId : resourceIds )
            {
                queueLock( resourceType, resourceId, false );
            }
        }

        private boolean queueLock( ResourceType resourceType, long resourceId, boolean exclusive )
        {
            // The contract is that after calling stop() no more locks should be acquired
            if ( !shouldStop )
            {
                locks.add( new Resource( resourceType, resourceId, exclusive ) );
            }
            return !shouldStop;
        }

        @Override
        public void acquireExclusive( ResourceType resourceType, long... resourceIds ) throws AcquireLockTimeoutException
        {
            for ( long resourceId : resourceIds )
            {
                queueLock( resourceType, resourceId, true );
            }
        }

        @Override
        public boolean tryExclusiveLock( ResourceType resourceType, long resourceId )
        {
            return queueLock( resourceType, resourceId, true );
        }

        @Override
        public boolean trySharedLock( ResourceType resourceType, long resourceId )
        {
            return queueLock( resourceType, resourceId, false );
        }

        @Override
        public void releaseShared( ResourceType resourceType, long resourceId )
        {
            locks.remove( new Resource( resourceType, resourceId, false ) );
        }

        @Override
        public void releaseExclusive( ResourceType resourceType, long resourceId )
        {
            locks.remove( new Resource( resourceType, resourceId, true ) );
        }

        @Override
        public void releaseAll()
        {
            throw new UnsupportedOperationException( "Should not be needed" );
        }

        @Override
        public void prepare()
        {
            long[] current = new long[10];
            int cursor = 0;
            ResourceType currentType = null;
            boolean currentExclusive = false;
            for ( Resource resource : locks )
            {
                // TODO perhaps also add a condition which sends batches over a certain size threshold
                if ( currentType == null ||
                        (currentType.typeId() != resource.resourceType.typeId() || currentExclusive != resource.exclusive) )
                {
                    // New type, i.e. flush the current array down to delegate in one call
                    if ( !flushLocks( current, cursor, currentType, currentExclusive ) )
                    {
                        break;
                    }

                    cursor = 0;
                    currentType = resource.resourceType;
                    currentExclusive = resource.exclusive;
                }

                // Queue into current batch
                if ( cursor == current.length )
                {
                    current = Arrays.copyOf( current, cursor*2 );
                }
                current[cursor++] = resource.resourceId;
            }
            flushLocks( current, cursor, currentType, currentExclusive );
        }

        private boolean flushLocks( long[] current, int cursor, ResourceType currentType, boolean currentExclusive )
        {
            if ( shouldStop )
            {
                return false;
            }
            if ( cursor > 0 )
            {
                long[] resourceIds = Arrays.copyOf( current, cursor );
                if ( currentExclusive )
                {
                    clientDelegate.acquireExclusive( currentType, resourceIds );
                }
                else
                {
                    clientDelegate.acquireShared( currentType, resourceIds );
                }
            }
            return true;
        }

        @Override
        public void stop()
        {
            shouldStop = true;
            clientDelegate.stop();
        }

        @Override
        public void close()
        {
            shouldStop = true;
            clientDelegate.close();
        }

        @Override
        public int getLockSessionId()
        {
            return clientDelegate.getLockSessionId();
        }

        @Override
        public Client delegate()
        {
            return clientDelegate;
        }
    }
}
