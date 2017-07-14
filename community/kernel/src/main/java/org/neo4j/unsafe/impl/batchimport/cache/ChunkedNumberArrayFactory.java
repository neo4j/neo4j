/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import static java.lang.Long.min;

/**
 * Used as part of the fallback strategy for {@link Auto}. Tries to split up fixed-size arrays
 * ({@link #newLongArray(long, long)} and {@link #newIntArray(long, int)} into smaller chunks where
 * some can live on heap and some off heap.
 */
public class ChunkedNumberArrayFactory extends NumberArrayFactory.Adapter
{
    private final NumberArrayFactory delegate;

    public ChunkedNumberArrayFactory()
    {
        this( OFF_HEAP, HEAP );
    }

    public ChunkedNumberArrayFactory( NumberArrayFactory... delegateList )
    {
        delegate = new Auto( delegateList );
    }

    @Override
    public LongArray newLongArray( long length, long defaultValue, long base )
    {
        // Here we want to have the property of a dynamic array which makes some parts of the array
        // live on heap, some off. At the same time we want a fixed size array. Therefore first create
        // the array as a dynamic array and make it grow to the requested length.
        LongArray array = newDynamicLongArray( fractionOf( length ), defaultValue );
        array.at( length - 1 );
        return array;
    }

    @Override
    public IntArray newIntArray( long length, int defaultValue, long base )
    {
        // Here we want to have the property of a dynamic array which makes some parts of the array
        // live on heap, some off. At the same time we want a fixed size array. Therefore first create
        // the array as a dynamic array and make it grow to the requested length.
        IntArray array = newDynamicIntArray( fractionOf( length ), defaultValue );
        array.at( length - 1 );
        return array;
    }

    @Override
    public ByteArray newByteArray( long length, byte[] defaultValue, long base )
    {
        // Here we want to have the property of a dynamic array which makes some parts of the array
        // live on heap, some off. At the same time we want a fixed size array. Therefore first create
        // the array as a dynamic array and make it grow to the requested length.
        ByteArray array = newDynamicByteArray( fractionOf( length ), defaultValue );
        array.at( length - 1 );
        return array;
    }

    private long fractionOf( long length )
    {
        int idealChunkCount = 10;
        if ( length < idealChunkCount )
        {
            return length;
        }
        int maxArraySize = Integer.MAX_VALUE - Short.MAX_VALUE;
        return min( length / idealChunkCount, maxArraySize );
    }

    @Override
    public IntArray newDynamicIntArray( long chunkSize, int defaultValue )
    {
        return new DynamicIntArray( delegate, chunkSize, defaultValue );
    }

    @Override
    public LongArray newDynamicLongArray( long chunkSize, long defaultValue )
    {
        return new DynamicLongArray( delegate, chunkSize, defaultValue );
    }

    @Override
    public ByteArray newDynamicByteArray( long chunkSize, byte[] defaultValue )
    {
        return new DynamicByteArray( delegate, chunkSize, defaultValue );
    }

    @Override
    public String toString()
    {
        return "CHUNKED_FIXED_SIZE";
    }
}
