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
package org.neo4j.io.pagecache.impl.muninn;

import org.neo4j.concurrent.BinaryLatch;
import org.neo4j.concurrent.jsr166e.StampedLock;
import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

/**
 * OptiLock is a sequence-based lock like StampedLock, but with the ability to take optimistic write-locks.
 * <p/>
 * The OptiLock supports non-blocking optimistic concurrent read-locks, and blocking concurrent write-locks,
 * and a pessimistic exclusive-lock.
 * <p/>
 * The optimistic read-lock works through validation, so at the end of the critical section, the read-lock has to be
 * validated and, if the validation fails, the critical section has to be retried. The read-lock acquires a stamp
 * at the start of the critical section, which is then validated at the end of the critical section. The stamp is
 * invalidated if any write-lock or exclusive lock has been taking since the stamp was acquired.
 * <p/>
 * The optimistic write-locks works by assuming that writes are always non-conflicting, so no validation is required.
 * However, the write-locks will check if a pessimistic exclusive lock is held at the start of the critical section,
 * and if so, block and wait for the exclusive lock to be released. The write-locks will invalidate all optimistic
 * read-locks.
 * <p/>
 * The exclusive lock will also invalidate the optimistic read-locks, but not the write locks. The exclusive lock is
 * try-lock only, and will never block.
 */
public class OptiLock
{
    /**
     * StampedLock methods used:
     *  - {@link StampedLock#writeLock()} - page faulting, write locking pages
     *  - {@link StampedLock#tryWriteLock()} - eviction
     *  - {@link StampedLock#readLock()} - flushing, pessimistic page read-locks
     *  - {@link StampedLock#tryReadLock()} - background flushing
     *  - {@link StampedLock#tryOptimisticRead()} - optimistic page read-locking
     *  - {@link StampedLock#validate(long stamp)} - optimistic page read-locking
     *  - {@link StampedLock#unlockRead(long stamp)} - flushing, pessimistic page read-locks
     *  - {@link StampedLock#unlockWrite(long stamp)} - eviction, page faulting, page write-locks
     *  - {@link StampedLock#tryConvertToReadLock(long stamp)} - page fault in page read cursor
     *  - {@link StampedLock#isWriteLocked()} - assertions
     *  - {@link StampedLock#isReadLocked()} - assertions
     */

    private static final long CNT_BITS = 17; // Bits for counting concurrent write-locks

    private static final long SEQ_BITS = 64 - 1 - CNT_BITS;
    private static final long CNT_UNIT = 1L << SEQ_BITS;
    private static final long SEQ_MASK = CNT_UNIT - 1L;
    private static final long SEQ_IMSK = ~SEQ_MASK;
    private static final long CNT_MASK = ((1L << CNT_BITS) - 1L) << SEQ_BITS;
    private static final long EXCL_MASK = (1L << CNT_BITS + SEQ_BITS);

    private static final long STATE = UnsafeUtil.getFieldOffset( OptiLock.class, "state" );

    @SuppressWarnings( "unused" ) // accessed via unsafe
    private volatile long state;
    private volatile BinaryLatch exclusiveLatch;

    private long getState()
    {
        return UnsafeUtil.getLongVolatile( this, STATE );
    }

    private boolean compareAndSetState( long expect, long update )
    {
        return UnsafeUtil.compareAndSwapLong( this, STATE, expect, update );
    }

    public long tryOptimisticReadLock()
    {
        return getState() & SEQ_MASK;
    }

    public boolean validateReadLock( long stamp )
    {
        UnsafeUtil.loadFence();
        return getState() == stamp;
    }

    public void writeLock()
    {
        long s, n;
        for (;;)
        {
            s = getState();
            if ( (s & EXCL_MASK) == EXCL_MASK )
            {
                blockOnExclusiveLock();
                continue;
            }
            if ( (s & CNT_MASK) == CNT_MASK )
            {
                throwWriteLockOverflow( s );
            }
            n = s + CNT_UNIT;
            if ( compareAndSetState( s, n ) )
            {
                return;
            }
        }
    }

    private void blockOnExclusiveLock()
    {
        BinaryLatch latch = exclusiveLatch;
        if ( latch != null )
        {
            latch.await( this );
        }
    }

    private long throwWriteLockOverflow( long s )
    {
        throw new IllegalMonitorStateException( "Write lock counter overflow: " + describeState( s ) );
    }

    public void unlockWrite()
    {
        long s, n;
        do
        {
            s = getState();
            if ( (s & CNT_MASK) == 0 )
            {
                throwUnmatchedUnlockWrite( s );
            }
            n = nextSeq( s ) - CNT_UNIT;
        }
        while ( !compareAndSetState( s, n ) );
    }

    private void throwUnmatchedUnlockWrite( long s )
    {
        throw new IllegalMonitorStateException( "Unmatched unlockWrite: " + describeState( s ) );
    }

    private long nextSeq( long s )
    {
        return (s & SEQ_IMSK) + (s + 1 & SEQ_MASK);
    }

    public boolean tryExclusiveLock()
    {
        long s = getState();
        if ( ((s & CNT_MASK) == 0) & ((s & EXCL_MASK) == 0) && compareAndSetState( s, s + EXCL_MASK ) )
        {
            exclusiveLatch = new BinaryLatch();
            return true;
        }
        return false;
    }

    public void unlockExclusive()
    {
        long s = getState();
        if ( (s & EXCL_MASK) != EXCL_MASK )
        {
            throwUnmatchedUnlockExclusive( s );
        }
        exclusiveLatch.release();
        exclusiveLatch = null;
        compareAndSetState( s, nextSeq( s ) - EXCL_MASK );
    }

    private void throwUnmatchedUnlockExclusive( long s )
    {
        throw new IllegalMonitorStateException( "Unmatched unlockExclusive: " + describeState( s ) );
    }

    @Override
    public String toString()
    {
        long s = getState();
        return describeState( s );
    }

    private String describeState( long s )
    {
        long excl = s >>> CNT_BITS + SEQ_BITS;
        long cnt = (s & CNT_MASK) >> SEQ_BITS;
        long seq = s & SEQ_MASK;
        StringBuilder sb = new StringBuilder( "OptiLock[E:" ).append( excl );
        sb.append( ", W:" ).append( Long.toBinaryString( cnt ) ).append( ':' ).append( cnt );
        sb.append( ", S:" ).append( Long.toBinaryString( seq ) ).append( ':' ).append( seq ).append( ']' );
        return sb.toString();
    }
}
