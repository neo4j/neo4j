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
package org.neo4j.kernel.impl.index.schema;

import java.nio.ByteBuffer;
import java.util.function.Supplier;

import org.neo4j.util.Preconditions;

import static java.lang.Math.toIntExact;

/**
 * Factory for {@link ByteBuffer} instances. The type of {@link ByteBuffer} allocated will be decided by the given {@link Allocator} passed into it.
 * This factory provides three differently scoped buffers:
 *
 * <ul>
 *     <li>{@link #globalAllocator() Global buffers} which will be closed when this factory {@link #close() closes}</li>
 *     <li>{@link #newLocalAllocator() Local buffers} where caller gets a new {@link Allocator} and gets the responsibility of its
 *     life cycle, i.e. allocations from it and must call {@link Allocator#close()} close on it after use</li>
 *     <li>{@link #acquireThreadLocalBuffer() Thread-local buffers} created lazily on first call by any given thread into this buffer factory.
 *     These buffers are allocated from the global allocator on first use and then only cleared and handed out on further requests.
 *     After use it must be {@link #releaseThreadLocalBuffer() released} so that other code paths in that thread's execution can acquire it.</li>
 * </ul>
 *
 * These scopes together allows for efficient allocation, de-allocation and sharing of buffers.
 */
public class ByteBufferFactory implements AutoCloseable
{
    private final Allocator globalAllocator;
    private final int threadLocalBufferSize;
    private final ThreadLocal<ThreadLocalByteBuffer> threadLocalBuffers = ThreadLocal.withInitial( ThreadLocalByteBuffer::new );
    private final Supplier<Allocator> allocatorFactory;

    public ByteBufferFactory( Supplier<Allocator> allocatorFactory, int threadLocalBufferSize )
    {
        this.allocatorFactory = allocatorFactory;
        this.globalAllocator = allocatorFactory.get();
        this.threadLocalBufferSize = threadLocalBufferSize;
    }

    /**
     * @return the global {@link Allocator} for private buffer allocation.
     */
    Allocator globalAllocator()
    {
        return globalAllocator;
    }

    /**
     * @return a new {@link Allocator} for local use. Must be closed by the caller when done.
     */
    Allocator newLocalAllocator()
    {
        return allocatorFactory.get();
    }

    /**
     * @return thread-local buffer. The returned buffer is meant to be used in a limited closure and then {@link #releaseThreadLocalBuffer() released}
     * so that other pieces of code can use it again for this thread.
     */
    ByteBuffer acquireThreadLocalBuffer()
    {
        return threadLocalBuffers.get().acquire();
    }

    /**
     * Releases a previously {@link #acquireThreadLocalBuffer() acquired} thread-local buffer.
     */
    void releaseThreadLocalBuffer()
    {
        ThreadLocalByteBuffer managedByteBuffer = threadLocalBuffers.get();
        Preconditions.checkState( managedByteBuffer != null, "Buffer doesn't exist" );
        managedByteBuffer.release();
    }

    public int bufferSize()
    {
        return threadLocalBufferSize;
    }

    @Override
    public void close()
    {
        globalAllocator.close();
    }

    public static ByteBufferFactory heapBufferFactory( int sharedBuffersSize )
    {
        return new ByteBufferFactory( () -> HEAP_ALLOCATOR, sharedBuffersSize );
    }

    /**
     * Allocator of {@link ByteBuffer} instances. Also is responsible for freeing memory of allocated buffers on {@link #close()}.
     */
    public interface Allocator extends AutoCloseable
    {
        ByteBuffer allocate( int bufferSize );

        @Override
        void close();
    }

    static Allocator HEAP_ALLOCATOR = new Allocator()
    {
        @Override
        public ByteBuffer allocate( int bufferSize )
        {
            return ByteBuffer.allocate( toIntExact( bufferSize ) );
        }

        @Override
        public void close()
        {
            // Nothing to close
        }
    };

    private class ThreadLocalByteBuffer
    {
        private boolean acquired;
        private ByteBuffer buffer;

        ByteBuffer acquire()
        {
            Preconditions.checkState( !acquired, "Already acquired" );
            acquired = true;
            if ( buffer == null )
            {
                buffer = globalAllocator.allocate( threadLocalBufferSize );
            }
            else
            {
                buffer.clear();
            }
            return buffer;
        }

        void release()
        {
            Preconditions.checkState( acquired, "Not acquired" );
            acquired = false;
        }
    }
}
