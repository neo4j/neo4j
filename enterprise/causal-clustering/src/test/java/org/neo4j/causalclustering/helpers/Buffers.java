/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.helpers;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.ReferenceCounted;
import org.junit.rules.ExternalResource;

import java.util.LinkedList;
import java.util.List;

/**
 * For tests that uses {@link ByteBuf}. All buffers that are allocated using {@link ByteBufAllocator} will be
 * released after test has executed.
 */
public class Buffers extends ExternalResource implements ByteBufAllocator
{
    private final ByteBufAllocator allocator;

    public Buffers( ByteBufAllocator allocator )
    {
        this.allocator = allocator;
    }

    private final List<ByteBuf> buffersList = new LinkedList<>();

    public Buffers()
    {
        this( new UnpooledByteBufAllocator( false ) );
    }

    public <BUFFER extends ByteBuf> BUFFER add( BUFFER byteBuf )
    {
        buffersList.add( byteBuf );
        return byteBuf;
    }

    @Override
    public ByteBuf buffer()
    {
        return add( allocator.buffer() );
    }

    @Override
    public ByteBuf buffer( int initialCapacity )
    {
        return add( allocator.buffer( initialCapacity ) );
    }

    @Override
    public ByteBuf buffer( int initialCapacity, int maxCapacity )
    {
        return add( allocator.buffer( initialCapacity, maxCapacity ) );
    }

    @Override
    public ByteBuf ioBuffer()
    {
        return add( allocator.ioBuffer() );
    }

    @Override
    public ByteBuf ioBuffer( int initialCapacity )
    {
        return add( allocator.ioBuffer( initialCapacity ) );
    }

    @Override
    public ByteBuf ioBuffer( int initialCapacity, int maxCapacity )
    {
        return add( allocator.ioBuffer( initialCapacity, maxCapacity ) );
    }

    @Override
    public ByteBuf heapBuffer()
    {
        return add( allocator.heapBuffer() );
    }

    @Override
    public ByteBuf heapBuffer( int initialCapacity )
    {
        return add( allocator.heapBuffer( initialCapacity ) );
    }

    @Override
    public ByteBuf heapBuffer( int initialCapacity, int maxCapacity )
    {
        return add( allocator.heapBuffer( initialCapacity, maxCapacity ) );
    }

    @Override
    public ByteBuf directBuffer()
    {
        return add( allocator.directBuffer() );
    }

    @Override
    public ByteBuf directBuffer( int initialCapacity )
    {
        return add( allocator.directBuffer( initialCapacity ) );
    }

    @Override
    public ByteBuf directBuffer( int initialCapacity, int maxCapacity )
    {
        return add( allocator.directBuffer( initialCapacity, maxCapacity ) );
    }

    @Override
    public CompositeByteBuf compositeBuffer()
    {
        return add( allocator.compositeBuffer() );
    }

    @Override
    public CompositeByteBuf compositeBuffer( int maxNumComponents )
    {
        return add( allocator.compositeBuffer( maxNumComponents ) );
    }

    @Override
    public CompositeByteBuf compositeHeapBuffer()
    {
        return add( allocator.compositeHeapBuffer() );
    }

    @Override
    public CompositeByteBuf compositeHeapBuffer( int maxNumComponents )
    {
        return add( allocator.compositeHeapBuffer( maxNumComponents ) );
    }

    @Override
    public CompositeByteBuf compositeDirectBuffer()
    {
        return add( allocator.compositeDirectBuffer() );
    }

    @Override
    public CompositeByteBuf compositeDirectBuffer( int maxNumComponents )
    {
        return add( allocator.compositeBuffer( maxNumComponents ) );
    }

    @Override
    public boolean isDirectBufferPooled()
    {
        return allocator.isDirectBufferPooled();
    }

    @Override
    public int calculateNewCapacity( int minNewCapacity, int maxCapacity )
    {
        return allocator.calculateNewCapacity( minNewCapacity, maxCapacity );
    }

    @Override
    protected void before()
    {
        buffersList.removeIf( buf -> buf.refCnt() == 0 );
    }

    @Override
    protected void after()
    {
        buffersList.forEach( ReferenceCounted::release );
    }
}
