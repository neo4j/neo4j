/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.internal.recordstorage;

import org.apache.commons.lang3.ArrayUtils;
import org.eclipse.collections.api.bag.sorted.MutableSortedBag;
import org.eclipse.collections.api.factory.SortedBags;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;

import org.neo4j.lock.ActiveLock;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.LockType;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.lock.ResourceType;
import org.neo4j.lock.ResourceTypes;
import org.neo4j.test.rule.RandomRule;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.lock.LockType.EXCLUSIVE;
import static org.neo4j.lock.LockType.SHARED;

class TrackingResourceLocker implements ResourceLocker
{
    private final Set<ResourceType> strictAssertions = new HashSet<>();
    private final HashMap<ResourceType,MutableSortedBag<Long>> exclusiveLocks = new HashMap<>();
    private final HashMap<ResourceType,MutableSortedBag<Long>> sharedLocks = new HashMap<>();
    private final RandomRule random;
    private final LockAcquisitionMonitor lockAcquisitionMonitor;
    private final int changeOfGettingTryLock;

    TrackingResourceLocker( RandomRule random, LockAcquisitionMonitor lockAcquisitionMonitor )
    {
        this( random, lockAcquisitionMonitor, random.nextInt( 1, 8 ) );  // i.e. 10%-90% chance
    }

    TrackingResourceLocker( RandomRule random, LockAcquisitionMonitor lockAcquisitionMonitor, int chanceOfGettingTrylockInPercent  )
    {
        this.random = random;
        this.lockAcquisitionMonitor = lockAcquisitionMonitor;
        this.changeOfGettingTryLock = chanceOfGettingTrylockInPercent;
    }

    TrackingResourceLocker withStrictAssertionsOn( ResourceType resourceType )
    {
        strictAssertions.add( resourceType );
        return this;
    }

    @Override
    public boolean tryExclusiveLock( ResourceType resourceType, long resourceId )
    {
        MutableSortedBag<Long> locks = locks( resourceType, exclusiveLocks );
        boolean hasLock = hasLock( resourceType, EXCLUSIVE, resourceId );
        boolean available = hasLock || random.nextInt( 100 ) < changeOfGettingTryLock;
        if ( available )
        {
            locks.add( resourceId );
            lockAcquisitionMonitor.lockAcquired( resourceType, EXCLUSIVE, resourceId, true );
        }
        return available;
    }

    @Override
    public void acquireExclusive( LockTracer tracer, ResourceType resourceType, long... resourceIds )
    {
        acquireLock( resourceType, resourceIds, EXCLUSIVE );
    }

    private void acquireLock( ResourceType resourceType, long[] resourceIds, LockType lockType )
    {
        MutableSortedBag<Long> locks = locks( resourceType, locksMap( lockType ) );
        boolean doStrictAssertions = strictAssertions.contains( resourceType );
        for ( long id : resourceIds )
        {
            if ( doStrictAssertions )
            {
                assertThat( locks.getLastOptional().orElse( -1L ) ).isLessThanOrEqualTo( id ); //sorted
            }
            boolean added = locks.add( id );
            if ( doStrictAssertions )
            {
                assertThat( added ).isTrue();
            }
            lockAcquisitionMonitor.lockAcquired( resourceType, lockType, id, false );
        }
    }

    @Override
    public void releaseExclusive( ResourceType resourceType, long... resourceIds )
    {
        releaseLock( resourceType, resourceIds, EXCLUSIVE );
    }

    private void releaseLock( ResourceType resourceType, long[] resourceIds, LockType lockType )
    {
        MutableSortedBag<Long> locks = locks( resourceType, locksMap( lockType ) );
        for ( long id : resourceIds )
        {
            boolean removed = locks.remove( id );
            assertThat( removed ).isTrue(); //should not unlock if not locked
            lockAcquisitionMonitor.lockReleased( resourceType, lockType, id, !locks.contains( id ) );
        }
    }

    @Override
    public void acquireShared( LockTracer tracer, ResourceType resourceType, long... resourceIds )
    {
        acquireLock( resourceType, resourceIds, SHARED );
    }

    @Override
    public void releaseShared( ResourceType resourceType, long... resourceIds )
    {
        releaseLock( resourceType, resourceIds, SHARED );
    }

    @Override
    public Stream<ActiveLock> activeLocks()
    {
        List<ActiveLock> locks = new ArrayList<>();
        gatherActiveLocks( locks, exclusiveLocks, EXCLUSIVE );
        gatherActiveLocks( locks, sharedLocks, LockType.SHARED );
        return locks.stream();
    }

    private void gatherActiveLocks( List<ActiveLock> locks, HashMap<ResourceType,MutableSortedBag<Long>> locksByType, LockType lockType )
    {
        locksByType.forEach(
                ( resourceType, resourceIds ) -> resourceIds.forEach( resourceId -> locks.add( new ActiveLock( resourceType, lockType, -1, resourceId ) ) ) );
    }

    private MutableSortedBag<Long> locks( ResourceType resourceType, HashMap<ResourceType,MutableSortedBag<Long>> locksByType )
    {
        return locksByType.computeIfAbsent( resourceType, type -> SortedBags.mutable.empty() );
    }

    LongSet getExclusiveLocks( ResourceType resourceType )
    {
        return LongSets.immutable.of( ArrayUtils.toPrimitive( locks( resourceType, exclusiveLocks ).toArray( new Long[0] ) ) );
    }

    boolean hasLock( ResourceType resourceType, LockType lockType, long resourceId )
    {
        return locks( resourceType, locksMap( lockType ) ).contains( resourceId );
    }

    boolean hasLock( ResourceType resourceType, long resourceId )
    {
        return locks( resourceType, locksMap( EXCLUSIVE ) ).contains( resourceId ) || locks( resourceType, locksMap( SHARED ) ).contains( resourceId );
    }

    void assertHasLock( ResourceType resourceType, LockType lockType, long resourceId )
    {
        assertThat( hasLock( resourceType, lockType, resourceId ) ).as( "Lock[%s,%s,%d]", resourceType, lockType, resourceId ).isTrue();
    }

    void assertNoLock( ResourceTypes resourceType, LockType lockType, long resourceId )
    {
        assertThat( hasLock( resourceType, lockType, resourceId ) ).as( "Lock[%s,%s,%d]", resourceType, lockType, resourceId ).isFalse();
    }

    private HashMap<ResourceType,MutableSortedBag<Long>> locksMap( LockType lockType )
    {
        switch ( lockType )
        {
        case EXCLUSIVE:
            return exclusiveLocks;
        case SHARED:
            return sharedLocks;
        default:
            throw new UnsupportedOperationException();
        }
    }

    ResourceLocker sortOfReadOnlyView()
    {
        TrackingResourceLocker actual = this;
        return new ResourceLocker()
        {
            @Override
            public boolean tryExclusiveLock( ResourceType resourceType, long resourceId )
            {
                return !actual.hasLock( resourceType, resourceId );
            }

            @Override
            public void acquireExclusive( LockTracer tracer, ResourceType resourceType, long... resourceIds )
            {
                for ( long resourceId : resourceIds )
                {
                    if ( actual.hasLock( resourceType, resourceId ) )
                    {
                        throw new AlreadyLockedException( resourceType + " " + resourceId + " is locked by someone else" );
                    }
                }
            }

            @Override
            public void releaseExclusive( ResourceType resourceType, long... resourceIds )
            {
            }

            @Override
            public void acquireShared( LockTracer tracer, ResourceType resourceType, long... resourceIds )
            {
                for ( long resourceId : resourceIds )
                {
                    if ( actual.hasLock( resourceType, EXCLUSIVE, resourceId ) )
                    {
                        throw new AlreadyLockedException( resourceType + " " + EXCLUSIVE + " " + resourceId + " is locked by someone else" );
                    }
                }
            }

            @Override
            public void releaseShared( ResourceType resourceType, long... resourceIds )
            {
            }

            @Override
            public Stream<ActiveLock> activeLocks()
            {
                return actual.activeLocks();
            }
        };
    }

    public interface LockAcquisitionMonitor
    {
        void lockAcquired( ResourceType resourceType, LockType lockType, long resourceId, boolean tryLocked );

        void lockReleased( ResourceType resourceType, LockType lockType, long resourceId, boolean wasTheLastOne );

        LockAcquisitionMonitor NO_MONITOR = new LockAcquisitionMonitor()
        {
            @Override
            public void lockAcquired( ResourceType resourceType, LockType lockType, long resourceId, boolean tryLocked )
            {
            }

            @Override
            public void lockReleased( ResourceType resourceType, LockType lockType, long resourceId, boolean wasTheLastOne )
            {
            }
        };
    }

    static class AlreadyLockedException extends RuntimeException
    {
        AlreadyLockedException( String message )
        {
            super( message );
        }
    }
}
