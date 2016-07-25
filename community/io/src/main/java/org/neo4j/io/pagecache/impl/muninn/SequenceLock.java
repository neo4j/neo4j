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
 * SequenceLock is a sequence-based lock like StampedLock, but entirely non-blocking, and with special lock modes that
 * are needed by the Muninn page cache.
 * <p>
 * The SequenceLock supports non-blocking optimistic concurrent read locks, non-blocking concurrent write locks,
 * and non-blocking pessimistic flush and exclusive locks.
 * <p>
 * The optimistic read lock works through validation, so at the end of the critical section, the read lock has to be
 * validated and, if the validation fails, the critical section has to be retried. The read-lock acquires a stamp
 * at the start of the critical section, which is then validated at the end of the critical section. The stamp is
 * invalidated if any write lock or exclusive lock was overlapping with the read lock.
 * <p>
 * The concurrent write locks works by assuming that writes are always non-conflicting, so no validation is required.
 * However, the write locks will check if a pessimistic exclusive lock is held at the start of the critical
 * section, and if so, fail to be acquired. The write locks will invalidate all optimistic read locks. The write lock
 * is try-lock only, and will never block.
 * <p>
 * The flush lock is also non-blocking (try-lock only), but can only be held by one thread at a time for
 * implementation reasons. The flush lock is meant for flushing the data in a page. This means that flush locks do not
 * invalidate optimistic read locks, nor does it prevent overlapping write locks. However, it does prevent other
 * overlapping flush and exclusive locks. Likewise, if another flush or exclusive lock is held, the attempt to take the
 * flush lock will fail.
 * <p>
 * The exclusive lock will also invalidate the optimistic read locks. The exclusive lock is try-lock only, and will
 * never block. If a write or flush lock is currently held, the attempt to take the exclusive lock will fail, and
 * the exclusive lock will likewise prevent write and flush locks from being taken.
 */
public class SequenceLock
{
    /*
     * Bits for counting concurrent write-locks. We use 17 bits because our pages are most likely 8192 bytes, and
     * 2^17 = 131.072, which is far more than our page size, so makes it highly unlikely that we are going to overflow
     * our concurrent write lock counter. Meanwhile, it's also small enough that we have a very large (2^45) number
     * space for our sequence. This one value controls the layout of the lock bit-state. The rest of the layout is
     * derived from this.
     *
     * With 17 writer count bits, the layout looks like this:
     *
     * ┏━ Flush lock bit
     * ┃┏━ Exclusive lock bit
     * ┃┃    ┏━ Count of currently concurrently held write locks, 17 bits.
     * ┃┃    ┃                  ┏━ 45 bits for the read lock sequence, incremented on write & exclusive unlock.
     * ┃┃┏━━━┻━━━━━━━━━━━━━┓┏━━━┻━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
     * FEWWWWWW WWWWWWWW WWWSSSSS SSSSSSSS SSSSSSSS SSSSSSSS SSSSSSSS SSSSSSSS
     * 1        2        3        4        5        6        7        8        byte
     */
    private static final long CNT_BITS = 17;

    private static final long BITS_IN_LONG = 64;
    private static final long EXL_LOCK_BITS = 1; // Exclusive lock bits (only 1 is supported)
    private static final long FLS_LOCK_BITS = 1; // Flush lock bits (only 1 is supported)
    private static final long SEQ_BITS = BITS_IN_LONG - FLS_LOCK_BITS - EXL_LOCK_BITS - CNT_BITS;
    private static final long CNT_UNIT = 1L << SEQ_BITS;
    private static final long SEQ_MASK = CNT_UNIT - 1L;
    private static final long SEQ_IMSK = ~SEQ_MASK;
    private static final long CNT_MASK = ((1L << CNT_BITS) - 1L) << SEQ_BITS;
    private static final long EXL_MASK = (1L << CNT_BITS + SEQ_BITS);
    private static final long FLS_MASK = (1L << CNT_BITS + SEQ_BITS + 1L);
    private static final long FLS_IMSK = ~FLS_MASK;
    private static final long FAE_MASK = FLS_MASK + EXL_MASK; // "flush and/or exclusive" mask
    private static final long UNL_MASK = FAE_MASK + CNT_MASK; // unlocked mask

    private static final long STATE = UnsafeUtil.getFieldOffset( SequenceLock.class, "state" );

    @SuppressWarnings( "unused" ) // accessed via unsafe
    private volatile long state;

    private long getState()
    {
        return state;
    }

    private boolean compareAndSetState( long expect, long update )
    {
        return UnsafeUtil.compareAndSwapLong( this, STATE, expect, update );
    }

    private void unconditionallySetState( long update )
    {
        state = update;
    }

    /**
     * Start an optimistic critical section, and return a stamp that can be used to validate if the read lock was
     * consistent. That is, if no write or exclusive lock was overlapping with the optimistic read lock.
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
     * the stamp.
     *
     * @param stamp The stamp of the optimistic read lock.
     * @return {@code true} if the optimistic read lock was valid, {@code false} otherwise.
     */
    public boolean validateReadLock( long stamp )
    {
        UnsafeUtil.loadFence();
        return (getState() & FLS_IMSK) == stamp;
    }

    /**
     * Try taking a concurrent write lock. Multiple write locks can be held at the same time. Write locks will
     * invalidate any optimistic read lock that overlaps with them, and write locks will make any attempt at grabbing
     * an exclusive lock fail. If an exclusive lock is currently held, then the attempt to take a write lock will fail.
     * <p>
     * Write locks must be paired with a corresponding {@link #unlockWrite()}.
     *
     * @return {@code true} if the write lock was taken, {@code false} otherwise.
     */
    public boolean tryWriteLock()
    {
        long s, n;
        for (; ; )
        {
            s = getState();
            boolean unwritablyLocked = (s & EXL_MASK) != 0;
            boolean writeCountOverflow = (s & CNT_MASK) == CNT_MASK;

            // bitwise-OR to reduce branching and allow more ILP
            if ( unwritablyLocked | writeCountOverflow )
            {
                return failWriteLock( s, writeCountOverflow );
            }

            n = s + CNT_UNIT;
            if ( compareAndSetState( s, n ) )
            {
                UnsafeUtil.storeFence();
                return true;
            }
        }
    }

    private boolean failWriteLock( long s, boolean writeCountOverflow )
    {
        if ( writeCountOverflow )
        {
            throwWriteLockOverflow( s );
        }
        // Otherwise it was exclusively locked
        return false;
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
     * Grab the exclusive lock if it is immediately available. Exclusive locks will invalidate any overlapping
     * optimistic read lock, and fail write and flush locks. If any write or flush locks are currently taken, or if
     * the exclusive lock is already taken, then the attempt to grab an exclusive lock will fail.
     * <p>
     * Successfully grabbed exclusive locks must always be paired with a corresponding {@link #unlockExclusive()}.
     *
     * @return {@code true} if we successfully got the exclusive lock, {@code false} otherwise.
     */
    public boolean tryExclusiveLock()
    {
        long s = getState();
        boolean res = ((s & UNL_MASK) == 0) && compareAndSetState( s, s + EXL_MASK );
        UnsafeUtil.storeFence();
        return res;
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
        long n = nextSeq( s ) - EXL_MASK;
        // Exclusive locks prevent any state modifications from write locks
        unconditionallySetState( n );
        return n;
    }

    /**
     * Atomically unlock the currently held exclusive lock, and take a write lock.
     */
    public void unlockExclusiveAndTakeWriteLock()
    {
        long s = initiateExclusiveLockRelease();
        long n = nextSeq( s ) - EXL_MASK + CNT_UNIT;
        unconditionallySetState( n );
    }

    private long initiateExclusiveLockRelease()
    {
        long s = getState();
        if ( (s & EXL_MASK) != EXL_MASK )
        {
            throwUnmatchedUnlockExclusive( s );
        }
        return s;
    }

    private void throwUnmatchedUnlockExclusive( long s )
    {
        throw new IllegalMonitorStateException( "Unmatched unlockExclusive: " + describeState( s ) );
    }

    /**
     * Grab the flush lock if it is immediately available. Flush locks prevent overlapping exclusive locks,
     * but do not invalidate optimistic read locks, nor do they prevent overlapping write locks. Only one flush lock
     * can be held at a time. If any flush or exclusive lock is already held, the attempt to take the flush lock will
     * fail.
     * <p>
     * Successfully grabbed flush locks must always be paired with a corresponding {@link #unlockFlush()}.
     *
     * @return {@code true} if we successfully got the flush lock, {@code false} otherwise.
     */
    public boolean tryFlushLock()
    {
        long s = getState();
        boolean res = ((s & FAE_MASK) == 0) && compareAndSetState( s, s + FLS_MASK );
        UnsafeUtil.storeFence();
        return res;
    }

    /**
     * Unlock the currently held flush lock.
     */
    public void unlockFlush()
    {
        long s, n;
        do
        {
            s = getState();
            if ( (s & FLS_MASK) != FLS_MASK )
            {
                throwUnmatchedUnlockFlush( s );
            }
            // We don't increment the sequence with nextSeq here, because flush locks don't invalidate readers
            n = s - FLS_MASK;
        }
        while ( !compareAndSetState( s, n ) );
    }

    private void throwUnmatchedUnlockFlush( long s )
    {
        throw new IllegalMonitorStateException( "Unmatched unlockFlush: " + describeState( s ) );
    }

    @Override
    public String toString()
    {
        long s = getState();
        return describeState( s );
    }

    private String describeState( long s )
    {
        long flush = s >>> CNT_BITS + SEQ_BITS + EXL_LOCK_BITS;
        long excl = (s & EXL_MASK) >>> CNT_BITS + SEQ_BITS;
        long cnt = (s & CNT_MASK) >> SEQ_BITS;
        long seq = s & SEQ_MASK;
        return "SequenceLock[Flush: " + flush + ", Excl: " + excl + ", Ws: " + cnt + ", S: " + seq + ']';
    }
}
