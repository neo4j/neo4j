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
package org.neo4j.internal.batchimport.cache;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Function;

import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.unsafe.NativeMemoryAllocationRefusedError;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.logging.Log;
import org.neo4j.memory.MemoryTracker;

import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.neo4j.io.ByteUnit.bytesToString;

/**
 * Factory of {@link LongArray}, {@link IntArray} and {@link ByteArray} instances. Users can select in which type of
 * memory the arrays will be placed, either in {@link #HEAP}, {@link #OFF_HEAP}, or use an auto allocator which
 * will have each instance placed where it fits best, favoring the primary candidates.
 */
public interface NumberArrayFactory
{
    interface Monitor
    {
        /**
         * Notifies about a successful allocation where information about both successful and failed attempts are included.
         *
         * @param memory amount of memory for this allocation.
         * @param successfulFactory the {@link NumberArrayFactory} which proved successful allocating this amount of memory.
         * @param attemptedAllocationFailures list of failed attempts to allocate this memory in other allocators.
         */
        void allocationSuccessful( long memory, NumberArrayFactory successfulFactory, Iterable<AllocationFailure> attemptedAllocationFailures );
    }

    Monitor NO_MONITOR = ( memory, successfulFactory, attemptedAllocationFailures ) -> { /* no-op */ };

    /**
     * Puts arrays inside the heap.
     */
    NumberArrayFactory HEAP = new Adapter()
    {
        @Override
        public IntArray newIntArray( long length, int defaultValue, long base, MemoryTracker memoryTracker )
        {
            return new HeapIntArray( toIntExact( length ), defaultValue, base, memoryTracker );
        }

        @Override
        public LongArray newLongArray( long length, long defaultValue, long base, MemoryTracker memoryTracker )
        {
            return new HeapLongArray( toIntExact( length ), defaultValue, base, memoryTracker );
        }

        @Override
        public HeapByteArray newByteArray( long length, byte[] defaultValue, long base, MemoryTracker memoryTracker )
        {
            return new HeapByteArray( toIntExact( length ), defaultValue, base, memoryTracker );
        }

        @Override
        public String toString()
        {
            return "HEAP";
        }
    };

    /**
     * Puts arrays off-heap, using unsafe calls.
     */
    NumberArrayFactory OFF_HEAP = new Adapter()
    {
        @Override
        public IntArray newIntArray( long length, int defaultValue, long base, MemoryTracker memoryTracker )
        {
            return new OffHeapIntArray( length, defaultValue, base, memoryTracker );
        }

        @Override
        public LongArray newLongArray( long length, long defaultValue, long base, MemoryTracker memoryTracker )
        {
            return new OffHeapLongArray( length, defaultValue, base, memoryTracker );
        }

        @Override
        public ByteArray newByteArray( long length, byte[] defaultValue, long base, MemoryTracker memoryTracker )
        {
            return new OffHeapByteArray( length, defaultValue, base, memoryTracker );
        }

        @Override
        public String toString()
        {
            return "OFF_HEAP";
        }
    };

    /**
     * Used as part of the fallback strategy for {@link Auto}. Tries to split up fixed-size arrays
     * ({@link #newLongArray(long, long, MemoryTracker)} and {@link #newIntArray(long, int, MemoryTracker)} into smaller chunks where
     * some can live on heap and some off heap.
     */
    NumberArrayFactory CHUNKED_FIXED_SIZE = new ChunkedNumberArrayFactory( NumberArrayFactory.NO_MONITOR );

    /**
     * {@link Auto} factory which uses JVM stats for gathering information about available memory.
     */
    NumberArrayFactory AUTO_WITHOUT_PAGECACHE = new Auto( NumberArrayFactory.NO_MONITOR, OFF_HEAP, HEAP, CHUNKED_FIXED_SIZE );

    /**
     * {@link Auto} factory which has a page cache backed number array as final fallback, in order to prevent OOM
     * errors.
     * @param pageCache {@link PageCache} to fallback allocation into, if no more memory is available.
     * @param pageCacheTracer underlying page cache events tracer
     * @param dir directory where cached files are placed.
     * @param allowHeapAllocation whether or not to allow allocation on heap. Otherwise allocation is restricted
     * to off-heap and the page cache fallback. This to be more in control of available space in the heap at all times.
     * @param monitor for monitoring successful and failed allocations and which factory was selected.
     * @return a {@link NumberArrayFactory} which tries to allocation off-heap, then potentially on heap
     * and lastly falls back to allocating inside the given {@code pageCache}.
     */
    static NumberArrayFactory auto( PageCache pageCache, PageCacheTracer pageCacheTracer,
            Path dir, boolean allowHeapAllocation, Monitor monitor, Log log )
    {
        PageCachedNumberArrayFactory pagedArrayFactory = new PageCachedNumberArrayFactory( pageCache, pageCacheTracer, dir, log );
        ChunkedNumberArrayFactory chunkedArrayFactory = new ChunkedNumberArrayFactory( monitor,
                allocationAlternatives( allowHeapAllocation, pagedArrayFactory ) );
        return new Auto( monitor, allocationAlternatives( allowHeapAllocation, chunkedArrayFactory ) );
    }

    /**
     * @param allowHeapAllocation whether or not to include heap allocation as an alternative.
     * @param additional other means of allocation to try after the standard off/on heap alternatives.
     * @return an array of {@link NumberArrayFactory} with the desired alternatives.
     */
    static NumberArrayFactory[] allocationAlternatives( boolean allowHeapAllocation, NumberArrayFactory... additional )
    {
        List<NumberArrayFactory> result = new ArrayList<>( Collections.singletonList( OFF_HEAP ) );
        if ( allowHeapAllocation )
        {
            result.add( HEAP );
        }
        result.addAll( asList( additional ) );
        return result.toArray( new NumberArrayFactory[0] );
    }

    class AllocationFailure
    {
        private final Throwable failure;
        private final NumberArrayFactory factory;

        AllocationFailure( Throwable failure, NumberArrayFactory factory )
        {
            this.failure = failure;
            this.factory = factory;
        }

        public Throwable getFailure()
        {
            return failure;
        }

        public NumberArrayFactory getFactory()
        {
            return factory;
        }
    }

    /**
     * Looks at available memory and decides where the requested array fits best. Tries to allocate the whole
     * array with the first candidate, falling back to others as needed.
     */
    class Auto extends Adapter
    {
        private final Monitor monitor;
        private final NumberArrayFactory[] candidates;

        public Auto( Monitor monitor, NumberArrayFactory... candidates )
        {
            Objects.requireNonNull( monitor );
            this.monitor = monitor;
            this.candidates = candidates;
        }

        @Override
        public LongArray newLongArray( long length, long defaultValue, long base, MemoryTracker memoryTracker )
        {
            return tryAllocate( length, 8, f -> f.newLongArray( length, defaultValue, base, memoryTracker ) );
        }

        @Override
        public IntArray newIntArray( long length, int defaultValue, long base, MemoryTracker memoryTracker )
        {
            return tryAllocate( length, 4, f -> f.newIntArray( length, defaultValue, base, memoryTracker ) );
        }

        @Override
        public ByteArray newByteArray( long length, byte[] defaultValue, long base, MemoryTracker memoryTracker )
        {
            return tryAllocate( length, defaultValue.length, f -> f.newByteArray( length, defaultValue, base, memoryTracker ) );
        }

        private <T extends NumberArray<? extends T>> T tryAllocate( long length, int itemSize,
                Function<NumberArrayFactory,T> allocator )
        {
            List<AllocationFailure> failures = new ArrayList<>();
            Error error = null;
            for ( NumberArrayFactory candidate : candidates )
            {
                try
                {
                    try
                    {
                        T array = allocator.apply( candidate );
                        monitor.allocationSuccessful( length * itemSize, candidate, failures );
                        return array;
                    }
                    catch ( ArithmeticException e )
                    {
                        throw new OutOfMemoryError( e.getMessage() );
                    }
                }
                catch ( OutOfMemoryError | NativeMemoryAllocationRefusedError e )
                {   // Alright let's try the next one
                    if ( error == null )
                    {
                        error = e;
                    }
                    else
                    {
                        e.addSuppressed( error );
                        error = e;
                    }
                    failures.add( new AllocationFailure( e, candidate ) );
                }
            }
            throw error( length, itemSize, error );
        }

        private Error error( long length, int itemSize, Error error )
        {
            return Exceptions.withMessage( error, format( "%s: Not enough memory available for allocating %s, tried %s",
                    error.getMessage(), bytesToString( length * itemSize ), Arrays.toString( candidates ) ) );
        }

    }

    /**
     * @param length size of the array.
     * @param defaultValue value which will represent unset values.
     * @param memoryTracker underlying buffers allocation memory tracker
     * @return a fixed size {@link IntArray}.
     */
    default IntArray newIntArray( long length, int defaultValue, MemoryTracker memoryTracker )
    {
        return newIntArray( length, defaultValue, 0, memoryTracker );
    }

    /**
     * @param length size of the array.
     * @param defaultValue value which will represent unset values.
     * @param base base index to rebase all requested indexes with.
     * @param memoryTracker underlying buffers allocation memory tracker
     * @return a fixed size {@link IntArray}.
     */
    IntArray newIntArray( long length, int defaultValue, long base, MemoryTracker memoryTracker );

    /**
     * @param chunkSize the size of each array (number of items). Where new chunks are added when needed.
     * @param defaultValue value which will represent unset values.
     * @param memoryTracker underlying buffers allocation memory tracker
     * @return dynamically growing {@link IntArray}.
     */
    IntArray newDynamicIntArray( long chunkSize, int defaultValue, MemoryTracker memoryTracker );

    /**
     * @param length size of the array.
     * @param defaultValue value which will represent unset values.
     * @param memoryTracker underlying buffers allocation memory tracker
     * @return a fixed size {@link LongArray}.
     */
    default LongArray newLongArray( long length, long defaultValue, MemoryTracker memoryTracker )
    {
        return newLongArray( length, defaultValue, 0, memoryTracker );
    }

    /**
     * @param length size of the array.
     * @param defaultValue value which will represent unset values.
     * @param base base index to rebase all requested indexes with.
     * @param memoryTracker underlying buffers allocation memory tracker
     * @return a fixed size {@link LongArray}.
     */
    LongArray newLongArray( long length, long defaultValue, long base, MemoryTracker memoryTracker );

    /**
     * @param chunkSize the size of each array (number of items). Where new chunks are added when needed.
     * @param defaultValue value which will represent unset values.
     * @param memoryTracker underlying buffers allocation memory tracker
     * @return dynamically growing {@link LongArray}.
     */
    LongArray newDynamicLongArray( long chunkSize, long defaultValue, MemoryTracker memoryTracker );

    /**
     * @param length size of the array.
     * @param defaultValue value which will represent unset values.
     * @param memoryTracker underlying buffers allocation memory tracker
     * @return a fixed size {@link ByteArray}.
     */
    default ByteArray newByteArray( long length, byte[] defaultValue, MemoryTracker memoryTracker )
    {
        return newByteArray( length, defaultValue, 0, memoryTracker );
    }

    /**
     * @param length size of the array.
     * @param defaultValue value which will represent unset values.
     * @param base base index to rebase all requested indexes with.
     * @param memoryTracker underlying buffers allocation memory tracker
     * @return a fixed size {@link ByteArray}.
     */
    ByteArray newByteArray( long length, byte[] defaultValue, long base, MemoryTracker memoryTracker );

    /**
     * @param chunkSize the size of each array (number of items). Where new chunks are added when needed.
     * @param defaultValue value which will represent unset values.
     * @param memoryTracker underlying buffers allocation memory tracker
     * @return dynamically growing {@link ByteArray}.
     */
    ByteArray newDynamicByteArray( long chunkSize, byte[] defaultValue, MemoryTracker memoryTracker );

    /**
     * Implements the dynamic array methods, because they are the same in most implementations.
     */

    abstract class Adapter implements NumberArrayFactory
    {
        @Override
        public IntArray newDynamicIntArray( long chunkSize, int defaultValue, MemoryTracker memoryTracker )
        {
            return new DynamicIntArray( this, chunkSize, defaultValue, memoryTracker );
        }

        @Override
        public LongArray newDynamicLongArray( long chunkSize, long defaultValue, MemoryTracker memoryTracker )
        {
            return new DynamicLongArray( this, chunkSize, defaultValue, memoryTracker );
        }

        @Override
        public ByteArray newDynamicByteArray( long chunkSize, byte[] defaultValue, MemoryTracker memoryTracker )
        {
            return new DynamicByteArray( this, chunkSize, defaultValue, memoryTracker );
        }
    }
}
