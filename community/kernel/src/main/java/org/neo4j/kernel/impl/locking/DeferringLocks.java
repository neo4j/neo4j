package org.neo4j.kernel.impl.locking;

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

    private static class Resource implements Comparable<Resource>
    {
        private final ResourceType resourceType;
        private final long resourceId;

        public Resource( ResourceType resourceType, long resourceId )
        {
            this.resourceType = resourceType;
            this.resourceId = resourceId;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            Resource resource = (Resource) o;
            if ( resourceId != resource.resourceId )
            {
                return false;
            }
            return resourceType.equals( resource.resourceType );
        }

        @Override
        public int hashCode()
        {
            int result = resourceType.hashCode();
            result = 31 * result + (int) (resourceId ^ (resourceId >>> 32));
            return result;
        }

        @Override
        public int compareTo( Resource o )
        {
            // The important thing isn't the order itself, it's the presence of an order
            // so that all lock clients gets the same order
            return resourceType.typeId() == o.resourceType.typeId() ? Long.compare( resourceId, o.resourceId )
                                                                    : resourceType.typeId() - o.resourceType.typeId();
        }
    }

    // TODO the state in this class is quite unoptimized, please do so
    private static class DeferringLockClient implements Client
    {
        private final Client clientDelegate;
        private final Set<Resource> shared = new TreeSet<>();
        private final Set<Resource> exclusive = new TreeSet<>();
        private boolean shouldStop;

        public DeferringLockClient( Client clientDelegate )
        {
            this.clientDelegate = clientDelegate;
        }

        @Override
        public void acquireShared( ResourceType resourceType, long resourceId ) throws AcquireLockTimeoutException
        {
            queueLock( resourceType, resourceId, shared );
        }

        private boolean queueLock( ResourceType resourceType, long resourceId, Set<Resource> lockSet )
        {
            // The contract is that after calling stop() no more locks should be acquired
            if ( !shouldStop )
            {
                lockSet.add( new Resource( resourceType, resourceId ) );
            }
            return !shouldStop;
        }

        @Override
        public void acquireExclusive( ResourceType resourceType, long resourceId ) throws AcquireLockTimeoutException
        {
            queueLock( resourceType, resourceId, exclusive );
        }

        @Override
        public boolean tryExclusiveLock( ResourceType resourceType, long resourceId )
        {
            return queueLock( resourceType, resourceId, exclusive );
        }

        @Override
        public boolean trySharedLock( ResourceType resourceType, long resourceId )
        {
            return queueLock( resourceType, resourceId, shared );
        }

        @Override
        public void releaseShared( ResourceType resourceType, long resourceId )
        {
            shared.remove( new Resource( resourceType, resourceId ) );
        }

        @Override
        public void releaseExclusive( ResourceType resourceType, long resourceId )
        {
            exclusive.remove( new Resource( resourceType, resourceId ) );
        }

        @Override
        public void releaseAll()
        {
            throw new UnsupportedOperationException( "Should not be needed" );
        }

        @Override
        public void prepare()
        {
            for ( Resource resource : shared )
            {
                if ( !shouldStop )
                {
                    clientDelegate.acquireShared( resource.resourceType, resource.resourceId );
                }
                else
                {
                    break;
                }
            }
            for ( Resource resource : exclusive )
            {
                if ( !shouldStop )
                {
                    clientDelegate.acquireExclusive( resource.resourceType, resource.resourceId );
                }
                else
                {
                    break;
                }
            }
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
