/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.io.pagecache.impl.muninn;

import org.neo4j.unsafe.impl.internal.dragons.UnsafeUtil;

/**
 * OffHeapPageLock is a sequence-based lock like StampedLock, but entirely non-blocking, and with special lock modes
 * that are needed by the Muninn page cache.
 * <p>
 * The OffHeapPageLock supports non-blocking optimistic concurrent read locks, non-blocking concurrent write locks,
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
 * is try-lock only, and will never block. A successfully taken write lock will raise the <em>modified</em> bit, if it
 * is not already raised. When the modified bit is raised, {@link #isModified(long)} will return {@code true}.
 * <p>
 * The flush lock is also non-blocking (try-lock only), but can only be held by one thread at a time for
 * implementation reasons. The flush lock is meant for flushing the data in a page. This means that flush locks do not
 * invalidate optimistic read locks, nor does it prevent overlapping write locks. However, it does prevent other
 * overlapping flush and exclusive locks. Likewise, if another flush or exclusive lock is held, the attempt to take the
 * flush lock will fail. The release of the flush lock will lower the modified bit if, and only if, it was not
 * overlapping with any write lock <em>and</em> the flush lock release method was given a success flag. The success flag
 * is necessary because the flush operation itself is an IO operation that may fail for any number of reasons.
 * <p>
 * The exclusive lock will also invalidate the optimistic read locks. The exclusive lock is try-lock only, and will
 * never block. If a write or flush lock is currently held, the attempt to take the exclusive lock will fail, and
 * the exclusive lock will likewise prevent write and flush locks from being taken.
 * <p>
 * Because all lock types are non-blocking, and because the lock-word itself is external to the implementation, this
 * class does not need to maintain any state by itself. Thus, the class cannot be instantiated, and all methods are
 * static.
 * <p>
 * Note that the lock-word is assumed to be 8 bytes, and should ideally be aligned to 8-byte boundaries, and ideally
 * run on platforms that support 8-byte-wide atomic memory operations.
 */
public final class OffHeapPageLock
{
    /*
     * Bits for counting concurrent write-locks. We use 17 bits because our pages are most likely 8192 bytes, and
     * 2^17 = 131.072, which is far more than our page size, so makes it highly unlikely that we are going to overflow
     * our concurrent write lock counter. Meanwhile, it's also small enough that we have a very large (2^44) number
     * space for our sequence. This one value controls the layout of the lock bit-state. The rest of the layout is
     * derived from this.
     *
     * With 17 writer count bits, the layout looks like this:
     *
     * ┏━ [FLS] Flush lock bit
     * ┃┏━ [EXL] Exclusive lock bit
     * ┃┃┏━ [MOD] Modified bit
     * ┃┃┃    ┏━ [CNT] Count of currently concurrently held write locks, 17 bits.
     * ┃┃┃    ┃                 ┏━ [SEQ] 44 bits for the read lock sequence, incremented on write & exclusive unlock.
     * ┃┃┃┏━━━┻━━━━━━━━━━━━━┓┏━━┻━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
     * FEMWWWWW WWWWWWWW WWWWSSSS SSSSSSSS SSSSSSSS SSSSSSSS SSSSSSSS SSSSSSSS
     * 1        2        3        4        5        6        7        8        byte
     */
    private static final long CNT_BITS = 17;

    private static final long BITS_IN_LONG = 64;
    private static final long EXL_LOCK_BITS = 1; // Exclusive lock bits (only 1 is supported)
    private static final long FLS_LOCK_BITS = 1; // Flush lock bits (only 1 is supported)
    private static final long MOD_BITS = 1; // Modified state bits (only 1 is supported)
    private static final long SEQ_BITS = BITS_IN_LONG - FLS_LOCK_BITS - EXL_LOCK_BITS - MOD_BITS - CNT_BITS;

    // Bit map reference:              = 0bFEMWWWWW WWWWWWWW WWWWSSSS SSSSSSSS SSSSSSSS SSSSSSSS SSSSSSSS SSSSSSSS
    private static final long FLS_MASK = 0b10000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000L;
    private static final long EXL_MASK = 0b01000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000L;
    private static final long MOD_MASK = 0b00100000_00000000_00000000_00000000_00000000_00000000_00000000_00000000L;
    private static final long CNT_MASK = 0b00011111_11111111_11110000_00000000_00000000_00000000_00000000_00000000L;
    private static final long SEQ_MASK = 0b00000000_00000000_00001111_11111111_11111111_11111111_11111111_11111111L;
    private static final long CNT_UNIT = 0b00000000_00000000_00010000_00000000_00000000_00000000_00000000_00000000L;
    private static final long SEQ_IMSK = 0b11111111_11111111_11110000_00000000_00000000_00000000_00000000_00000000L;
    // Mask used to check optimistic read lock validity:
    private static final long CHK_MASK = 0b01011111_11111111_11111111_11111111_11111111_11111111_11111111_11111111L;
    // "Flush and/or exclusive" mask:
    private static final long FAE_MASK = 0b11000000_00000000_00000000_00000000_00000000_00000000_00000000_00000000L;
    // Unlocked mask:
    private static final long UNL_MASK = 0b11011111_11111111_11110000_00000000_00000000_00000000_00000000_00000000L;

    private OffHeapPageLock()
    {
        // The static version keeps all state externally.
    }

    private static long getState( long address )
    {
        return UnsafeUtil.getLongVolatile( address );
    }

    private static boolean compareAndSetState( long address, long expect, long update )
    {
        return UnsafeUtil.compareAndSwapLong( null, address, expect, update );
    }

    private static void unconditionallySetState( long address, long update )
    {
        UnsafeUtil.putLongVolatile( address, update );
    }

    /**
     * @return A newly initialised lock word, for a lock that is exclusively locked.
     */
    public static long initialLockWordWithExclusiveLock()
    {
        return EXL_MASK;
    }

    /**
     * Start an optimistic critical section, and return a stamp that can be used to validate if the read lock was
     * consistent. That is, if no write or exclusive lock was overlapping with the optimistic read lock.
     *
     * @return A stamp that must be passed to {@link #validateReadLock(long, long)} to validate the critical section.
     */
    public static long tryOptimisticReadLock( long address )
    {
        return getState( address ) & SEQ_MASK;
    }

    /**
     * Validate a stamp from {@link #tryOptimisticReadLock(long)} or {@link #unlockExclusive(long)}, and return
     * {@code true} if no write or exclusive lock overlapped with the critical section of the optimistic read lock
     * represented by the stamp.
     *
     * @param stamp The stamp of the optimistic read lock.
     * @return {@code true} if the optimistic read lock was valid, {@code false} otherwise.
     */
    public static boolean validateReadLock( long address, long stamp )
    {
        UnsafeUtil.loadFence();
        return (getState( address ) & CHK_MASK) == stamp;
    }

    public static boolean isModified( long address )
    {
        return (getState( address ) & MOD_MASK) == MOD_MASK;
    }

    public static boolean isExclusivelyLocked( long address )
    {
        return (getState( address ) & EXL_MASK) == EXL_MASK;
    }

    /**
     * Try taking a concurrent write lock. Multiple write locks can be held at the same time. Write locks will
     * invalidate any optimistic read lock that overlaps with them, and write locks will make any attempt at grabbing
     * an exclusive lock fail. If an exclusive lock is currently held, then the attempt to take a write lock will fail.
     * <p>
     * Write locks must be paired with a corresponding {@link #unlockWrite(long)}.
     *
     * @return {@code true} if the write lock was taken, {@code false} otherwise.
     */
    public static boolean tryWriteLock( long address )
    {
        long s;
        long n;
        for ( ; ; )
        {
            s = getState( address );
            boolean unwritablyLocked = (s & EXL_MASK) != 0;
            boolean writeCountOverflow = (s & CNT_MASK) == CNT_MASK;

            // bitwise-OR to reduce branching and allow more ILP
            if ( unwritablyLocked | writeCountOverflow )
            {
                return failWriteLock( s, writeCountOverflow );
            }

            n = s + CNT_UNIT | MOD_MASK;
            if ( compareAndSetState( address, s, n ) )
            {
                UnsafeUtil.storeFence();
                return true;
            }
        }
    }

    private static boolean failWriteLock( long s, boolean writeCountOverflow )
    {
        if ( writeCountOverflow )
        {
            throwWriteLockOverflow( s );
        }
        // Otherwise it was exclusively locked
        return false;
    }

    private static void throwWriteLockOverflow( long s )
    {
        throw new IllegalMonitorStateException( "Write lock counter overflow: " + describeState( s ) );
    }

    /**
     * Release a write lock taking with {@link #tryWriteLock(long)}.
     */
    public static void unlockWrite( long address )
    {
        long s;
        long n;
        do
        {
            s = getState( address );
            if ( (s & CNT_MASK) == 0 )
            {
                throwUnmatchedUnlockWrite( s );
            }
            n = nextSeq( s ) - CNT_UNIT;
        }
        while ( !compareAndSetState( address, s, n ) );
    }

    private static void throwUnmatchedUnlockWrite( long s )
    {
        throw new IllegalMonitorStateException( "Unmatched unlockWrite: " + describeState( s ) );
    }

    private static long nextSeq( long s )
    {
        return (s & SEQ_IMSK) + (s + 1 & SEQ_MASK);
    }

    public static long unlockWriteAndTryTakeFlushLock( long address )
    {
        long s;
        long n;
        long r;
        do
        {
            r = 0;
            s = getState( address );
            if ( (s & CNT_MASK) == 0 )
            {
                throwUnmatchedUnlockWrite( s );
            }
            n = nextSeq( s ) - CNT_UNIT;
            if ( (n & FAE_MASK) == 0 )
            {
                n += FLS_MASK;
                r = n;
            }
        }
        while ( !compareAndSetState( address, s, n ) );
        UnsafeUtil.storeFence();
        return r;
    }

    /**
     * Grab the exclusive lock if it is immediately available. Exclusive locks will invalidate any overlapping
     * optimistic read lock, and fail write and flush locks. If any write or flush locks are currently taken, or if
     * the exclusive lock is already taken, then the attempt to grab an exclusive lock will fail.
     * <p>
     * Successfully grabbed exclusive locks must always be paired with a corresponding {@link #unlockExclusive(long)}.
     *
     * @return {@code true} if we successfully got the exclusive lock, {@code false} otherwise.
     */
    public static boolean tryExclusiveLock( long address )
    {
        long s = getState( address );
        boolean res = ((s & UNL_MASK) == 0) && compareAndSetState( address, s, s + EXL_MASK );
        UnsafeUtil.storeFence();
        return res;
    }

    /**
     * Unlock the currently held exclusive lock, and atomically and implicitly take an optimistic read lock, as
     * represented by the returned stamp.
     *
     * @return A stamp that represents an optimistic read lock, in case you need it.
     */
    public static long unlockExclusive( long address )
    {
        long s = initiateExclusiveLockRelease( address );
        long n = nextSeq( s ) - EXL_MASK;
        // Exclusive locks prevent any state modifications from write locks
        unconditionallySetState( address, n );
        return n;
    }

    /**
     * Atomically unlock the currently held exclusive lock, and take a write lock.
     */
    public static void unlockExclusiveAndTakeWriteLock( long address )
    {
        long s = initiateExclusiveLockRelease( address );
        long n = (nextSeq( s ) - EXL_MASK + CNT_UNIT) | MOD_MASK;
        unconditionallySetState( address, n );
    }

    private static long initiateExclusiveLockRelease( long address )
    {
        long s = getState( address );
        if ( (s & EXL_MASK) != EXL_MASK )
        {
            throwUnmatchedUnlockExclusive( s );
        }
        return s;
    }

    private static void throwUnmatchedUnlockExclusive( long s )
    {
        throw new IllegalMonitorStateException( "Unmatched unlockExclusive: " + describeState( s ) );
    }

    /**
     * If the given lock is exclusively held, then the <em>modified</em> flag will be explicitly lowered (marked as
     * unmodified) if the <em>modified</em> is currently raised.
     * <p>
     * If the <em>modified</em> flag is currently not raised, then this method does nothing.
     *
     * @throws IllegalStateException if the lock at the given address is not in the exclusively locked state.
     */
    public static void explicitlyMarkPageUnmodifiedUnderExclusiveLock( long address )
    {
        long s = getState( address );
        if ( (s & EXL_MASK) != EXL_MASK )
        {
            throw new IllegalStateException( "Page must be exclusively locked to explicitly lower modified bit" );
        }
        s = s & (~MOD_MASK);
        unconditionallySetState( address, s );
    }

    /**
     * Grab the flush lock if it is immediately available. Flush locks prevent overlapping exclusive locks,
     * but do not invalidate optimistic read locks, nor do they prevent overlapping write locks. Only one flush lock
     * can be held at a time. If any flush or exclusive lock is already held, the attempt to take the flush lock will
     * fail.
     * <p>
     * Successfully grabbed flush locks must always be paired with a corresponding
     * {@link #unlockFlush(long, long, boolean)}.
     *
     * @return If the lock is successfully grabbed, the method will return a stamp value that must be passed to the
     * {@link #unlockFlush(long, long, boolean)}, and which is used for detecting any overlapping write locks. If the
     * flush lock could not be taken, {@code 0} will be returned.
     */
    public static long tryFlushLock( long address )
    {
        long s = getState( address );
        if ( (s & FAE_MASK) == 0 )
        {
            long n = s + FLS_MASK;
            boolean res = compareAndSetState( address, s, n );
            UnsafeUtil.storeFence();
            return res ? n : 0;
        }
        return 0;
    }

    /**
     * Unlock the currently held flush lock.
     */
    public static void unlockFlush( long address, long stamp, boolean success )
    {
        long s;
        long n;
        do
        {
            s = getState( address );
            if ( (s & FLS_MASK) != FLS_MASK )
            {
                throwUnmatchedUnlockFlush( s );
            }
            // We don't increment the sequence with nextSeq here, because flush locks don't invalidate readers
            n = s - FLS_MASK;
            if ( success && (s & CHK_MASK) == (stamp & SEQ_MASK) )
            {
                // The flush was successful and we had no overlapping writers, thus we can lower the modified flag
                n = n & (~MOD_MASK);
            }
        }
        while ( !compareAndSetState( address, s, n ) );
    }

    private static void throwUnmatchedUnlockFlush( long s )
    {
        throw new IllegalMonitorStateException( "Unmatched unlockFlush: " + describeState( s ) );
    }

    private static String describeState( long s )
    {
        long flush = s >>> EXL_LOCK_BITS + MOD_BITS + CNT_BITS + SEQ_BITS;
        long excl = (s & EXL_MASK) >>> MOD_BITS + CNT_BITS + SEQ_BITS;
        long mod = (s & MOD_MASK) >>> CNT_BITS + SEQ_BITS;
        long cnt = (s & CNT_MASK) >> SEQ_BITS;
        long seq = s & SEQ_MASK;
        return "OffHeapPageLock[" +
               "Flush: " + flush + ", Excl: " + excl + ", Mod: " + mod + ", Ws: " + cnt + ", S: " + seq + "]";
    }

    static String toString( long address )
    {
        return describeState( getState( address ) );
    }
}
