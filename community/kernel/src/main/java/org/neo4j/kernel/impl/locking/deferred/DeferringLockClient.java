package org.neo4j.kernel.impl.locking.deferred;

import java.util.Arrays;
import java.util.Set;
import java.util.TreeSet;

import org.neo4j.kernel.impl.locking.AcquireLockTimeoutException;
import org.neo4j.kernel.impl.locking.Locks;

// TODO the state keeping in this class is quite unoptimized, please do so
public class DeferringLockClient implements Locks.Client
{
    private final Locks.Client clientDelegate;
    private final Set<LockUnit> locks = new TreeSet<>();
    private boolean shouldStop;

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

    private boolean queueLock( Locks.ResourceType resourceType, long resourceId, boolean exclusive )
    {
        // The contract is that after calling stop() no more locks should be acquired
        if ( !shouldStop )
        {
            locks.add( new LockUnit( resourceType, resourceId, exclusive ) );
        }
        return !shouldStop;
    }

    @Override
    public void acquireExclusive( Locks.ResourceType resourceType, long... resourceIds ) throws AcquireLockTimeoutException
    {
        for ( long resourceId : resourceIds )
        {
            queueLock( resourceType, resourceId, true );
        }
    }

    @Override
    public boolean tryExclusiveLock( Locks.ResourceType resourceType, long resourceId )
    {
        return queueLock( resourceType, resourceId, true );
    }

    @Override
    public boolean trySharedLock( Locks.ResourceType resourceType, long resourceId )
    {
        return queueLock( resourceType, resourceId, false );
    }

    @Override
    public void releaseShared( Locks.ResourceType resourceType, long resourceId )
    {
        locks.remove( new LockUnit( resourceType, resourceId, false ) );
    }

    @Override
    public void releaseExclusive( Locks.ResourceType resourceType, long resourceId )
    {
        locks.remove( new LockUnit( resourceType, resourceId, true ) );
    }

    @Override
    public void releaseAll()
    {
        throw new UnsupportedOperationException( "Should not be needed" );
    }

    public void grabDeferredLocks()
    {
        long[] current = new long[10];
        int cursor = 0;
        Locks.ResourceType currentType = null;
        boolean currentExclusive = false;
        for ( LockUnit lockUnit : locks )
        {
            // TODO perhaps also add a condition which sends batches over a certain size threshold
            if ( currentType == null ||
                    (currentType.typeId() != lockUnit.resourceType().typeId() || currentExclusive != lockUnit.isExclusive()) )
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
}
