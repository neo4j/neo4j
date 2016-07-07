package org.neo4j.kernel.impl.locking.deferred;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import org.neo4j.kernel.impl.locking.AcquireLockTimeoutException;
import org.neo4j.kernel.impl.locking.LockClientStoppedException;
import org.neo4j.kernel.impl.locking.Locks;

// TODO the state keeping in this class is quite unoptimized, please do so
public class DeferringLockClient implements Locks.Client
{
    private final Locks.Client clientDelegate;
    private final Set<LockUnit> locks = new TreeSet<>();
    private volatile boolean stopped;

    public DeferringLockClient( Locks.Client clientDelegate )
    {
        this.clientDelegate = clientDelegate;
    }

    @Override
    public void acquireShared( Locks.ResourceType resourceType, long... resourceIds ) throws AcquireLockTimeoutException
    {
        for ( long resourceId : resourceIds )
        {
            queueLock( resourceType, resourceId, false );
        }
    }

    private void queueLock( Locks.ResourceType resourceType, long resourceId, boolean exclusive )
    {
        assertNotStopped();
        locks.add( new LockUnit( resourceType, resourceId, exclusive ) );
    }

    @Override
    public void acquireExclusive( Locks.ResourceType resourceType, long... resourceIds )
            throws AcquireLockTimeoutException
    {
        for ( long resourceId : resourceIds )
        {
            queueLock( resourceType, resourceId, true );
        }
    }

    @Override
    public boolean tryExclusiveLock( Locks.ResourceType resourceType, long resourceId )
    {
        throw new UnsupportedOperationException( "Should not be needed" );
    }

    @Override
    public boolean trySharedLock( Locks.ResourceType resourceType, long resourceId )
    {
        throw new UnsupportedOperationException( "Should not be needed" );
    }

    @Override
    public void releaseShared( Locks.ResourceType resourceType, long resourceId )
    {
        final LockUnit unit = new LockUnit( resourceType, resourceId, false );
        if ( !locks.remove( unit ) )
        {
            throw new IllegalStateException( "Cannot release lock that it does not hold: " +
                                             resourceType + "[" + resourceId + "]." );
        }
    }

    @Override
    public void releaseExclusive( Locks.ResourceType resourceType, long resourceId )
    {
        final LockUnit unit = new LockUnit( resourceType, resourceId, true );
        if ( !locks.remove( unit ) )
        {
            throw new IllegalStateException( "Cannot release lock that it does not hold: " +
                                             resourceType + "[" + resourceId + "]." );
        }
    }

    void acquireDeferredLocks()
    {
        long[] current = new long[10];
        int cursor = 0;
        Locks.ResourceType currentType = null;
        boolean currentExclusive = false;
        for ( LockUnit lockUnit : locks )
        {
            // TODO perhaps also add a condition which sends batches over a certain size threshold
            if ( currentType == null ||
                 (currentType.typeId() != lockUnit.resourceType().typeId() ||
                  currentExclusive != lockUnit.isExclusive()) )
            {
                // New type, i.e. flush the current array down to delegate in one call
                if ( !flushLocks( current, cursor, currentType, currentExclusive ) )
                {
                    break;
                }

                cursor = 0;
                currentType = lockUnit.resourceType();
                currentExclusive = lockUnit.isExclusive();
            }

            // Queue into current batch
            if ( cursor == current.length )
            {
                current = Arrays.copyOf( current, cursor * 2 );
            }
            current[cursor++] = lockUnit.resourceId();
        }
        flushLocks( current, cursor, currentType, currentExclusive );
    }

    private boolean flushLocks( long[] current, int cursor, Locks.ResourceType currentType, boolean currentExclusive )
    {
        assertNotStopped();

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
        stopped = true;
        clientDelegate.stop();
    }

    @Override
    public void close()
    {
        stopped = true;
        clientDelegate.close();
    }

    @Override
    public int getLockSessionId()
    {
        return clientDelegate.getLockSessionId();
    }

    private void assertNotStopped() {
        if( stopped ) {
            throw new LockClientStoppedException( this );
        }
    }
}
