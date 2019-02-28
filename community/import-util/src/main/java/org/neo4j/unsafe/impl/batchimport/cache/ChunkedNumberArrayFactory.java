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
package org.neo4j.unsafe.impl.batchimport.cache;

import static java.lang.Long.min;

/**
 * Used as part of the fallback strategy for {@link Auto}. Tries to split up fixed-size arrays
 * ({@link #newLongArray(long, long)} and {@link #newIntArray(long, int)} into smaller chunks where
 * some can live on heap and some off heap.
 */
public class ChunkedNumberArrayFactory extends NumberArrayFactory.Adapter
{
    static final int MAGIC_CHUNK_COUNT = 10;
    // This is a safe bet on the maximum number of items the JVM can store in an array. It is commonly slightly less
    // than Integer.MAX_VALUE
    private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - Short.MAX_VALUE;
    private final NumberArrayFactory delegate;

    ChunkedNumberArrayFactory( Monitor monitor )
    {
        this( monitor, OFF_HEAP, HEAP );
    }

    ChunkedNumberArrayFactory( Monitor monitor, NumberArrayFactory... delegateList )
    {
        delegate = new Auto( monitor, delegateList );
    }

    @Override
    public LongArray newLongArray( long length, long defaultValue, long base )
    {
        // Here we want to have the property of a dynamic array so that some parts of the array
        // can live on heap, some off.
        return newDynamicLongArray( fractionOf( length ), defaultValue );
    }

    @Override
    public IntArray newIntArray( long length, int defaultValue, long base )
    {
        // Here we want to have the property of a dynamic array so that some parts of the array
        // can live on heap, some off.
        return newDynamicIntArray( fractionOf( length ), defaultValue );
    }

    @Override
    public ByteArray newByteArray( long length, byte[] defaultValue, long base )
    {
        // Here we want to have the property of a dynamic array so that some parts of the array
        // can live on heap, some off.
        return newDynamicByteArray( fractionOf( length ), defaultValue );
    }

    private long fractionOf( long length )
    {
        if ( length < MAGIC_CHUNK_COUNT )
        {
            return length;
        }
        return min( length / MAGIC_CHUNK_COUNT, MAX_ARRAY_SIZE );
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
        return "ChunkedNumberArrayFactory with delegate " + delegate;
    }
}
