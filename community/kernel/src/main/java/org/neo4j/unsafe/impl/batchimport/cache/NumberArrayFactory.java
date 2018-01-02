/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.cache;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.neo4j.helpers.Exceptions;
import org.neo4j.io.pagecache.PageCache;

import static java.lang.String.format;
import static java.util.Arrays.asList;

import static org.neo4j.helpers.Exceptions.launderedException;
import static org.neo4j.helpers.Format.bytes;
import static org.neo4j.helpers.Numbers.safeCastLongToInt;

/**
 * Factory of {@link LongArray}, {@link IntArray} and {@link ByteArray} instances. Users can select in which type of
 * memory the arrays will be placed, either in {@link #HEAP}, {@link #OFF_HEAP}, or use an auto allocator which
 * will have each instance placed where it fits best, favoring the primary candidates.
 */
public interface NumberArrayFactory
{
    /**
     * Puts arrays inside the heap.
     */
    NumberArrayFactory HEAP = new Adapter()
    {
        @Override
        public IntArray newIntArray( long length, int defaultValue, long base )
        {
            return new HeapIntArray( safeCastLongToInt( length ), defaultValue, base );
        }

        @Override
        public LongArray newLongArray( long length, long defaultValue, long base )
        {
            return new HeapLongArray( safeCastLongToInt( length ), defaultValue, base );
        }

        @Override
        public ByteArray newByteArray( long length, byte[] defaultValue, long base )
        {
            return new HeapByteArray( safeCastLongToInt( length ), defaultValue, base );
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
        public IntArray newIntArray( long length, int defaultValue, long base )
        {
            return new OffHeapIntArray( length, defaultValue, base );
        }

        @Override
        public LongArray newLongArray( long length, long defaultValue, long base )
        {
            return new OffHeapLongArray( length, defaultValue, base );
        }

        @Override
        public ByteArray newByteArray( long length, byte[] defaultValue, long base )
        {
            return new OffHeapByteArray( length, defaultValue, base );
        }

        @Override
        public String toString()
        {
            return "OFF_HEAP";
        }
    };

    /**
     * Used as part of the fallback strategy for {@link Auto}. Tries to split up fixed-size arrays
     * ({@link #newLongArray(long, long)} and {@link #newIntArray(long, int)} into smaller chunks where
     * some can live on heap and some off heap.
     */
    NumberArrayFactory CHUNKED_FIXED_SIZE = new ChunkedNumberArrayFactory();

    /**
     * {@link Auto} factory which uses JVM stats for gathering information about available memory.
     */
    NumberArrayFactory AUTO_WITHOUT_PAGECACHE = new Auto( OFF_HEAP, HEAP, CHUNKED_FIXED_SIZE );

    /**
     * {@link Auto} factory which has a page cache backed number array as final fallback, in order to prevent OOM
     * errors.
     * @param pageCache {@link PageCache} to fallback allocation into, if no more memory is available.
     * @param dir directory where cached files are placed.
     * @param allowHeapAllocation whether or not to allow allocation on heap. Otherwise allocation is restricted
     * to off-heap and the page cache fallback. This to be more in control of available space in the heap at all times.
     * @return a {@link NumberArrayFactory} which tries to allocation off-heap, then potentially on heap
     * and lastly falls back to allocating inside the given {@code pageCache}.
     */
    static NumberArrayFactory auto( PageCache pageCache, File dir, boolean allowHeapAllocation )
    {
        PageCachedNumberArrayFactory pagedArrayFactory = new PageCachedNumberArrayFactory( pageCache, dir );
        ChunkedNumberArrayFactory chunkedArrayFactory = new ChunkedNumberArrayFactory(
                allocationAlternatives( allowHeapAllocation, pagedArrayFactory ) );
        return new Auto( allocationAlternatives( allowHeapAllocation, chunkedArrayFactory ) );
    }

    /**
     * @param allowHeapAllocation whether or not to include heap allocation as an alternative.
     * @param additional other means of allocation to try after the standard off/on heap alternatives.
     * @return an array of {@link NumberArrayFactory} with the desired alternatives.
     */
    static NumberArrayFactory[] allocationAlternatives( boolean allowHeapAllocation, NumberArrayFactory... additional )
    {
        List<NumberArrayFactory> result = new ArrayList<>( asList( OFF_HEAP ) );
        if ( allowHeapAllocation )
        {
            result.add( HEAP );
        }
        result.addAll( asList( additional ) );
        return result.toArray( new NumberArrayFactory[result.size()] );
    }

    /**
     * Looks at available memory and decides where the requested array fits best. Tries to allocate the whole
     * array with the first candidate, falling back to others as needed.
     */
    class Auto extends Adapter
    {

        private final NumberArrayFactory[] candidates;

        public Auto( NumberArrayFactory... candidates )
        {
            this.candidates = candidates;
        }

        @Override
        public LongArray newLongArray( long length, long defaultValue, long base )
        {
            Throwable error = null;
            for ( NumberArrayFactory candidate : candidates )
            {
                try
                {
                    return candidate.newLongArray( length, defaultValue, base );
                }
                catch ( Throwable e )
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
                }
            }
            throw launderedException( error( length, 8, error ) );
        }

        @Override
        public IntArray newIntArray( long length, int defaultValue, long base )
        {
            Throwable error = null;
            for ( NumberArrayFactory candidate : candidates )
            {
                try
                {
                    return candidate.newIntArray( length, defaultValue, base );
                }
                catch ( Throwable e )
                {   // Allright let's try the next one
                    error = e;
                }
            }
            throw launderedException( error( length, 4, error ) );
        }

        @Override
        public ByteArray newByteArray( long length, byte[] defaultValue, long base )
        {
            Throwable error = null;
            for ( NumberArrayFactory candidate : candidates )
            {
                try
                {
                    return candidate.newByteArray( length, defaultValue, base );
                }
                catch ( Throwable e )
                {   // Allright let's try the next one
                    error = e;
                }
            }
            throw launderedException( error( length, defaultValue.length, error ) );
        }
        private Throwable error( long length, int itemSize, Throwable error )
        {
            return Exceptions.withMessage( error, format( "%s: Not enough memory available for allocating %s, tried %s",
                    error.getMessage(), bytes( length * itemSize ), Arrays.toString( candidates ) ) );
        }

    }

    /**
     * @param length size of the array.
     * @param defaultValue value which will represent unset values.
     * @return a fixed size {@link IntArray}.
     */
    default IntArray newIntArray( long length, int defaultValue )
    {
        return newIntArray( length, defaultValue, 0 );
    }

    /**
     * @param length size of the array.
     * @param defaultValue value which will represent unset values.
     * @param base base index to rebase all requested indexes with.
     * @return a fixed size {@link IntArray}.
     */
    IntArray newIntArray( long length, int defaultValue, long base );

    /**
     * @param chunkSize the size of each array (number of items). Where new chunks are added when needed.
     * @param defaultValue value which will represent unset values.
     * @return dynamically growing {@link IntArray}.
     */
    IntArray newDynamicIntArray( long chunkSize, int defaultValue );

    /**
     * @param length size of the array.
     * @param defaultValue value which will represent unset values.
     * @return a fixed size {@link LongArray}.
     */
    default LongArray newLongArray( long length, long defaultValue )
    {
        return newLongArray( length, defaultValue, 0 );
    }

    /**
     * @param length size of the array.
     * @param defaultValue value which will represent unset values.
     * @param base base index to rebase all requested indexes with.
     * @return a fixed size {@link LongArray}.
     */
    LongArray newLongArray( long length, long defaultValue, long base );

    /**
     * @param chunkSize the size of each array (number of items). Where new chunks are added when needed.
     * @param defaultValue value which will represent unset values.
     * @return dynamically growing {@link LongArray}.
     */
    LongArray newDynamicLongArray( long chunkSize, long defaultValue );

    /**
     * @param length size of the array.
     * @param defaultValue value which will represent unset values.
     * @return a fixed size {@link ByteArray}.
     */
    default ByteArray newByteArray( long length, byte[] defaultValue )
    {
        return newByteArray( length, defaultValue, 0 );
    }

    /**
     * @param length size of the array.
     * @param defaultValue value which will represent unset values.
     * @param base base index to rebase all requested indexes with.
     * @return a fixed size {@link ByteArray}.
     */
    ByteArray newByteArray( long length, byte[] defaultValue, long base );

    /**
     * @param chunkSize the size of each array (number of items). Where new chunks are added when needed.
     * @param defaultValue value which will represent unset values.
     * @return dynamically growing {@link ByteArray}.
     */
    ByteArray newDynamicByteArray( long chunkSize, byte[] defaultValue );

    /**
     * Implements the dynamic array methods, because they are the same in most implementations.
     */

    abstract class Adapter implements NumberArrayFactory
    {

        @Override
        public IntArray newDynamicIntArray( long chunkSize, int defaultValue )
        {
            return new DynamicIntArray( this, chunkSize, defaultValue );
        }

        @Override
        public LongArray newDynamicLongArray( long chunkSize, long defaultValue )
        {
            return new DynamicLongArray( this, chunkSize, defaultValue );
        }

        @Override
        public ByteArray newDynamicByteArray( long chunkSize, byte[] defaultValue )
        {
            return new DynamicByteArray( this, chunkSize, defaultValue );
        }

    }
}
