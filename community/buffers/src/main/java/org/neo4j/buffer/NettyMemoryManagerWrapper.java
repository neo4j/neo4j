/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.buffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.UnpooledDirectByteBuf;
import io.netty.buffer.UnpooledHeapByteBuf;

import java.nio.ByteBuffer;

import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.bufferpool.ByteBufferManger;

public class NettyMemoryManagerWrapper implements ByteBufAllocator
{
    // The following constants are shamelessly copied from Netty's AbstractByteBufAllocator
    private static final int DEFAULT_INITIAL_CAPACITY = 256;
    private static final int DEFAULT_MAX_CAPACITY = Integer.MAX_VALUE;
    private static final int DEFAULT_MAX_COMPONENTS = 16;
    private static final int CALCULATE_THRESHOLD = (int) ByteUnit.kibiBytes( 512 );

    private final ByteBufferManger pooledBufferManger;
    private final boolean delegateForCapacityCalculation;

    public NettyMemoryManagerWrapper( ByteBufferManger pooledBufferManger )
    {
        this( pooledBufferManger, true );
    }

    private NettyMemoryManagerWrapper( ByteBufferManger pooledBufferManger, boolean delegateForCapacityCalculation )
    {
        assert UnsafeUtil.unsafeByteBufferAccessAvailable() : "Unsafe ByteBuffer access is required for NettyMemoryManagerWrapper" ;
        this.pooledBufferManger = pooledBufferManger;
        this.delegateForCapacityCalculation = delegateForCapacityCalculation;
    }

    @Override
    public ByteBuf buffer()
    {
        return directBuffer();
    }

    @Override
    public ByteBuf buffer( int initialCapacity )
    {
        return directBuffer( initialCapacity );
    }

    @Override
    public ByteBuf buffer( int initialCapacity, int maxCapacity )
    {
        return directBuffer( initialCapacity, maxCapacity );
    }

    @Override
    public ByteBuf ioBuffer()
    {
        return directBuffer();
    }

    @Override
    public ByteBuf ioBuffer( int initialCapacity )
    {
        return directBuffer( initialCapacity );
    }

    @Override
    public ByteBuf ioBuffer( int initialCapacity, int maxCapacity )
    {
        return directBuffer( initialCapacity, maxCapacity );
    }

    @Override
    public ByteBuf heapBuffer()
    {
        return heapBuffer( DEFAULT_INITIAL_CAPACITY );
    }

    @Override
    public ByteBuf heapBuffer( int initialCapacity )
    {
        return heapBuffer( initialCapacity, DEFAULT_MAX_CAPACITY );
    }

    @Override
    public ByteBuf heapBuffer( int initialCapacity, int maxCapacity )
    {
        // Allocating a reasonably small heap buffer is fine without pooling.
        // Allocating a large one is currently considered a "soft" bug and should be very rare
        // So there is no point doing anything smart performance-wise.
        // If this is used on a hot code path, it should be detected in benchmarks and fixed
        // by switching to direct buffers
        return new HeapBuffer( initialCapacity, maxCapacity );
    }

    @Override
    public ByteBuf directBuffer()
    {
        return newDirectBuffer( DEFAULT_INITIAL_CAPACITY, false, DEFAULT_MAX_CAPACITY );
    }

    @Override
    public ByteBuf directBuffer( int initialCapacity )
    {
        return directBuffer( initialCapacity, DEFAULT_MAX_CAPACITY );
    }

    @Override
    public ByteBuf directBuffer( int initialCapacity, int maxCapacity )
    {
        return newDirectBuffer( initialCapacity, true, maxCapacity );
    }

    @Override
    public CompositeByteBuf compositeBuffer()
    {
        return compositeBuffer( DEFAULT_MAX_COMPONENTS );
    }

    @Override
    public CompositeByteBuf compositeBuffer( int maxNumComponents )
    {
        return compositeDirectBuffer( maxNumComponents );
    }

    @Override
    public CompositeByteBuf compositeHeapBuffer()
    {
        return compositeHeapBuffer( DEFAULT_MAX_COMPONENTS );
    }

    @Override
    public CompositeByteBuf compositeHeapBuffer( int maxNumComponents )
    {
        return new CompositeByteBuf( new HeapBufferAllocator( pooledBufferManger ), false, maxNumComponents );
    }

    @Override
    public CompositeByteBuf compositeDirectBuffer()
    {
        return compositeDirectBuffer( DEFAULT_MAX_COMPONENTS );
    }

    @Override
    public CompositeByteBuf compositeDirectBuffer( int maxNumComponents )
    {
        return new CompositeByteBuf( this, true, maxNumComponents );
    }

    @Override
    public boolean isDirectBufferPooled()
    {
        return true;
    }

    @Override
    public int calculateNewCapacity( int minNewCapacity, int maxCapacity )
    {
        if ( delegateForCapacityCalculation )
        {
            int capacityFromPool = pooledBufferManger.recommendNewCapacity( minNewCapacity, maxCapacity );
            if ( capacityFromPool != -1 )
            {
                return capacityFromPool;
            }
        }

        // The following comes from Netty's AbstractByteBufAllocator
        // in other words, if our buffer manager has no opinion
        // about ideal next capacity, we fallback to Netty's logic.
        // Too bad that method is not static.

        if ( minNewCapacity == CALCULATE_THRESHOLD )
        {
            return CALCULATE_THRESHOLD;
        }

        // If over threshold, do not double but just increase by threshold.
        if ( minNewCapacity > CALCULATE_THRESHOLD )
        {
            int newCapacity = minNewCapacity / CALCULATE_THRESHOLD * CALCULATE_THRESHOLD;
            if ( newCapacity > maxCapacity - CALCULATE_THRESHOLD )
            {
                newCapacity = maxCapacity;
            }
            else
            {
                newCapacity += CALCULATE_THRESHOLD;
            }
            return newCapacity;
        }

        // Not over threshold. Double up to 512 KiB, starting from 64.
        int newCapacity = 64;
        while ( newCapacity < minNewCapacity )
        {
            newCapacity <<= 1;
        }

        return Math.min( newCapacity, maxCapacity );
    }

    // What does 'forceInitialCapacity' stand for?
    // It is a necessary evil because we reuse UnpooledDirectByteBuf,
    // which has operations that use capacity of the underlying NIO buffer
    // to set capacity of the Netty Buffer.
    // Why is that bad?
    // Because ByteBufferManger implementation can
    // return a buffer which is larger than the required capacity.
    // So?
    // There is code both in Netty core and Neo networking stack that assumes,
    // that when executing allocator.buffer(123) it will get buffer with capacity
    // exactly 123 bytes and that code will not work if the returned buffer
    // has larger capacity. Even thought it is not specified what the 'initialCapacity'
    // argument on allocator methods means, our ByteBuf must behave like the Netty ones.
    // And 'forceInitialCapacity'?
    // It specifies that the 'initialCapacity' argument comes from the allocator caller
    // and therefore the underlying NIO buffer cannot have capacity larger than that.
    private ByteBuf newDirectBuffer( int initialCapacity, boolean forceInitialCapacity, int maxCapacity )
    {
        return new UnpooledDirectByteBuf( this, initialCapacity, maxCapacity )
        {
            // Getting the original buffer from a buffer slice
            // is not super cheap, so in order to avoid that operation
            // in unnecessary cases, this flag is used to track if slicing
            // has ever been used during the lifetime of this ByteBuf instance.
            private boolean slicingUsed;

            @Override
            protected ByteBuffer allocateDirect( int initialCapacity )
            {
                var buffer = pooledBufferManger.acquire( initialCapacity );
                if ( forceInitialCapacity && buffer.capacity() > initialCapacity )
                {
                    slicingUsed = true;
                    return buffer.slice();
                }

                return buffer;
            }

            @Override
            protected void freeDirect( ByteBuffer buffer )
            {
                if ( slicingUsed )
                {
                    ByteBuffer original = UnsafeUtil.getOriginalBufferFromSlice( buffer );
                    // original == null means that the submitted buffer wasn't a slice
                    if ( original != null )
                    {
                        pooledBufferManger.release( original );
                        return;
                    }
                }

                pooledBufferManger.release( buffer );
            }
        };
    }

    private class HeapBuffer extends UnpooledHeapByteBuf
    {

        HeapBuffer( int initialCapacity, int maxCapacity )
        {
            super( new HeapBufferAllocator( pooledBufferManger ), initialCapacity, maxCapacity );
        }

        @Override
        protected byte[] allocateArray( int initialCapacity )
        {
            pooledBufferManger.getHeapBufferMemoryTracker().allocateHeap( initialCapacity );
            return super.allocateArray( initialCapacity );
        }

        @Override
        protected void freeArray( byte[] array )
        {
            int length = array.length;
            super.freeArray( array );
            pooledBufferManger.getHeapBufferMemoryTracker().releaseHeap( length );
        }
    }

    private static class HeapBufferAllocator extends NettyMemoryManagerWrapper
    {

        HeapBufferAllocator( ByteBufferManger pooledBufferManger )
        {
            super( pooledBufferManger, false );
        }
    }
}
