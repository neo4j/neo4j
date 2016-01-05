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
package org.neo4j.io.pagecache.impl.muninn;

import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

/**
 * OptiLock is a sequence-based lock like StampedLock, but with the ability to take optimistic write-locks.
 * <p>
 * The OptiLock supports non-blocking optimistic concurrent read-locks, and blocking concurrent write-locks,
 * and a pessimistic exclusive-lock.
 * <p>
 * The optimistic read-lock works through validation, so at the end of the critical section, the read-lock has to be
 * validated and, if the validation fails, the critical section has to be retried. The read-lock acquires a stamp
 * at the start of the critical section, which is then validated at the end of the critical section. The stamp is
 * invalidated if any write-lock or exclusive lock has been taking since the stamp was acquired.
 * <p>
 * The optimistic write-locks works by assuming that writes are always non-conflicting, so no validation is required.
 * However, the write-locks will check if a pessimistic exclusive lock is held at the start of the critical section,
 * and if so, block and wait for the exclusive lock to be released. The write-locks will invalidate all optimistic
 * read-locks.
 * <p>
 * The exclusive lock will also invalidate the optimistic read-locks, but not the write locks. The exclusive lock is
 * try-lock only, and will never block.
 */
public class OptiLock
{
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

    private long getState()
    {
        return UnsafeUtil.getLongVolatile( this, STATE );
    }

    private boolean compareAndSetState( long expect, long update )
    {
        return UnsafeUtil.compareAndSwapLong( this, STATE, expect, update );
    }

    /**
     * Start an optimistic critical section, and return a stamp that can be used to validate if the read-lock was
     * consistent. That is, if no write or exclusive lock was overlapping with the optimistic read-lock.
     *
     * @return A stamp that must be passed to {@link #validateReadLock(long)} to validate the critical section.
     */
    public long tryOptimisticReadLock()
    {
        return getState() & SEQ_MASK;
    }

    /**
     * Validate a stamp from {@link #tryOptimisticReadLock()} or {@link #unlockExclusive()}, and return {@code true}
     * if no write or exclusive lock overlapped with the critical section of the optimistic read lock represented by
     * the
     * stamp.
     *
     * @param stamp The stamp of the optimistic read lock.
     * @return {@code true} if the optimistic read lock was valid, {@code false} otherwise.
     */
    public boolean validateReadLock( long stamp )
    {
        UnsafeUtil.loadFence();
        return getState() == stamp;
    }

    /**
     * Try taking a concurrent write lock. Multiple write locks can be held at the same time. Write locks will
     * invalidate any optimistic read lock that overlaps with them, and write locks will make any attempt at grabbing
     * an exclusive lock fail. If an exclusive lock is currently held, then the attempt to take a write lock will fail.
     * <p>
     * Write locks must be paired with a corresponding {@link #unlockWrite()}.
     * @return {@code true} if the write lock was taken, {@code false} otherwise.
     */
    public boolean tryWriteLock()
    {
        long s, n;
        for (; ; )
        {
            s = getState();
            if ( (s & EXCL_MASK) == EXCL_MASK )
            {
                return false;
            }
            if ( (s & CNT_MASK) == CNT_MASK )
            {
                throwWriteLockOverflow( s );
            }
            n = s + CNT_UNIT;
            if ( compareAndSetState( s, n ) )
            {
                return true;
            }
        }
    }

    private long throwWriteLockOverflow( long s )
    {
        throw new IllegalMonitorStateException( "Write lock counter overflow: " + describeState( s ) );
    }

    /**
     * Release a write lock taking with {@link #tryWriteLock()}.
     */
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

    /**
     * Grab an exclusive lock if one is immediately available. Exclusive locks will invalidate any overlapping
     * optimistic read lock, and make write locks block. If any write locks are currently taken, then the attempt to
     * grab an exclusive lock will fail.
     * <p>
     * Successfully grabbed exclusive locks must always be paired with a corresponding {@link #unlockExclusive()}.
     *
     * @return {@code true} if we successfully got an exclusive lock, {@code false} otherwise.
     */
    public boolean tryExclusiveLock()
    {
        long s = getState();
        return ((s & CNT_MASK) == 0) & ((s & EXCL_MASK) == 0) && compareAndSetState( s, s + EXCL_MASK );
    }

    /**
     * Unlock the currently held exclusive lock, and atomically and implicitly take an optimistic read lock, as
     * represented by the returned stamp.
     *
     * @return A stamp that represents an optimistic read lock, in case you need it.
     */
    public long unlockExclusive()
    {
        long s = initiateExclusiveLockRelease();
        long n = nextSeq( s ) - EXCL_MASK;
        compareAndSetState( s, n );
        return n;
    }

    /**
     * Atomically unlock the currently held exclusive lock, and take a write lock.
     */
    public void unlockExclusiveAndTakeWriteLock()
    {
        long s = initiateExclusiveLockRelease();
        long n = nextSeq( s ) - EXCL_MASK + CNT_UNIT;
        compareAndSetState( s, n );
    }

    private long initiateExclusiveLockRelease()
    {
        long s = getState();
        if ( (s & EXCL_MASK) != EXCL_MASK )
        {
            throwUnmatchedUnlockExclusive( s );
        }
        return s;
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
