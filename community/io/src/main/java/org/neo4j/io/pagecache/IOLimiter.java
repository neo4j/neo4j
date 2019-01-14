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
package org.neo4j.io.pagecache;

import java.io.Flushable;
import java.io.IOException;

/**
 * IOLimiter instances can be passed to the {@link PageCache#flushAndForce(IOLimiter)} and
 * {@link PagedFile#flushAndForce(IOLimiter)} methods, which will invoke the
 * {@link #maybeLimitIO(long, int, Flushable)} method on regular intervals.
 * <p/>
 * This allows the limiter to measure the rate of IO, and inject sleeps, pauses or flushes into the process.
 * The flushes are in this case referring to the underlying hardware.
 * <p/>
 * Normally, flushing a channel will just copy the dirty buffers into the OS page cache, but flushing is in this case
 * implying that the OS pages are cleared as well. In other words, the IOPSLimiter can make sure that the operating
 * system does not pile up too much IO work in its page cache, by flushing those caches as well on regular intervals.
 */
public interface IOLimiter
{
    /**
     * The value of the initial stamp; that is, what should be passed as the {@code previousStamp} to
     * {@link #maybeLimitIO(long, int, Flushable)} on the first call in a flush.
     */
    long INITIAL_STAMP = 0;

    /**
     * Invoked at regular intervals during flushing of the {@link PageCache} or {@link PagedFile}s.
     * <p/>
     * For the first call in a flush, the {@code previousStamp} should have the {@link #INITIAL_STAMP} value.
     * The return value of this method should then be used as the stamp of the next call. This allows implementations
     * to be stateless, yet still keep some context around about a given flush, provided they can encode it as a
     * {@code long}.
     * <p/>
     * The meaning of this long value is totally opaque to the caller, and can be anything the IOPSLimiter
     * implementation desires.
     * <p/>
     * The implementation is allowed to force changes to the storage device using the given {@link Flushable}, or
     * to perform {@link Thread#sleep(long) sleeps}, as it desires. It is not allowed to throw
     * {@link InterruptedException}, however. Those should be dealt with by catching them and re-interrupting the
     * current thread, or by wrapping them in {@link IOException}s.
     *
     * @param previousStamp The stamp from the previous call to this method, or {@link #INITIAL_STAMP} if this is the
     * first call to this method for the given flush.
     * @param recentlyCompletedIOs The number of IOs completed since the last call to this method.
     * @param flushable A {@link Flushable} instance that can flush any relevant dirty system buffers, to help smooth
     * out the IO load on the storage device.
     * @return A new stamp to pass into the next call to this method.
     */
    long maybeLimitIO( long previousStamp, int recentlyCompletedIOs, Flushable flushable );

    /**
     * Temporarily disable the IOLimiter, to allow IO to proceed at full speed.
     * This call <strong>MUST</strong> be paired with a subsequent {@link #enableLimit()} call.
     * This method is thread-safe and reentrant. Multiple concurrent calls will "stack", and IO limitations will be
     * enabled again once the last overlapping limit-disabling period ends with the "last" call to
     * {@link #enableLimit()}. This is conceptually similar to how a reentrant read-lock works.
     *
     * Thus, the typical usage pattern is with a {@code try-finally} clause, like this:
     *
     * <pre><code>
     *     limiter.disableLimit();
     *     try
     *     {
     *         // ... do work that needs maximum IO performance ...
     *     }
     *     finally
     *     {
     *         limiter.enableLimit();
     *     }
     * </code></pre>
     */
    default void disableLimit()
    {
        // By default this method does nothing, assuming the implementation always has no or fixed limits.
    }

    /**
     * Re-enable the IOLimiter, after having disabled it with {@link #disableLimit()}.
     *
     * @see #disableLimit() for how to use this method.
     */
    default void enableLimit()
    {
        // Same as for disableLimit().
    }

    /**
     * An IOPSLimiter implementation that does not restrict the rate of IO. Use this implementation if you want the
     * flush to go as fast as possible.
     */
    static IOLimiter unlimited()
    {
        return ( previousStamp, recentlyCompletedIOs, flushable ) -> previousStamp;
    }
}
