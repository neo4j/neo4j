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
package org.neo4j.internal.id;

import java.util.Arrays;
import java.util.Deque;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.function.Supplier;

import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import static java.util.Arrays.copyOf;

/**
 * Buffer of {@code long} values which can be held until a safe threshold is crossed, at which point
 * they are released onto a {@link Consumer}. These values are held in chunk and each chunk knows
 * which threshold is considered safe. Regular {@link #maintenance(PageCursorTracer)} is required to be called externally
 * for values to be released.
 *
 * This class is thread-safe for concurrent requests, but only a single thread should be responsible for
 * calling {@link #maintenance(PageCursorTracer)}.
 */
public class DelayedBuffer<T>
{
    private static class Chunk<T>
    {
        private final T threshold;
        private final long[] values;

        Chunk( T threshold, long[] values )
        {
            this.threshold = threshold;
            this.values = values;
        }

        @Override
        public String toString()
        {
            return Arrays.toString( values );
        }
    }

    private final Supplier<T> thresholdSupplier;
    private final Predicate<T> safeThreshold;
    private final ChunkConsumer chunkConsumer;
    private final Deque<Chunk<T>> chunks = new ConcurrentLinkedDeque<>();
    private final int chunkSize;
    private final Lock consumeLock = new ReentrantLock();

    private final long[] chunk;
    private int chunkCursor;

    DelayedBuffer( Supplier<T> thresholdSupplier, Predicate<T> safeThreshold, int chunkSize,
            ChunkConsumer chunkConsumer )
    {
        assert chunkSize > 0;
        this.thresholdSupplier = thresholdSupplier;
        this.safeThreshold = safeThreshold;
        this.chunkSize = chunkSize;
        this.chunkConsumer = chunkConsumer;
        this.chunk = new long[chunkSize];
    }

    /**
     * Should be called every now and then to check for safe thresholds of buffered chunks and potentially
     * release them onto the {@link Consumer}.
     */
    public void maintenance( PageCursorTracer cursorTracer )
    {
        synchronized ( this )
        {
            flush();
        }

        consumeLock.lock();
        try
        {
            // Potentially hand over chunks to the consumer
            while ( !chunks.isEmpty() )
            {
                Chunk<T> candidate = chunks.peek();
                if ( safeThreshold.test( candidate.threshold ) )
                {
                    chunkConsumer.consume( candidate.values, cursorTracer );
                    chunks.remove();
                }
                else
                {
                    // The chunks are ordered by chunkThreshold, so we know that no more chunks will qualify anyway
                    break;
                }
            }
        }
        finally
        {
            consumeLock.unlock();
        }
    }

    // Must be called under synchronized on this
    private void flush()
    {
        if ( chunkCursor > 0 )
        {
            synchronized ( chunks )
            {
                Chunk<T> chunkToAdd = new Chunk<>( thresholdSupplier.get(), copyOf( chunk, chunkCursor ) );
                chunks.offer( chunkToAdd );
            }
            chunkCursor = 0;
        }
    }

    /**
     * Offers a value to this buffer. This value will at a later point be part of a buffered chunk,
     * released by a call to {@link #maintenance(PageCursorTracer)} when the safe threshold for the chunk, which is determined
     * when the chunk is full or otherwise queued.
     */
    public synchronized void offer( long value )
    {
        chunk[chunkCursor++] = value;
        if ( chunkCursor == chunkSize )
        {
            flush();
        }
    }

    /**
     * Closes this buffer, releasing all {@link #offer(long)} values into the {@link Consumer}.
     *
     * This class is typically not used in a scenario suitable for try-with-resource
     * and so having it implement AutoCloseable would be more annoying
     */
    public synchronized void close()
    {
        clear();
    }

    public synchronized void clear()
    {
        chunks.clear();
        chunkCursor = 0;
    }
}
