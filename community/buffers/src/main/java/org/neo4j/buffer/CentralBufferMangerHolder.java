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

import io.netty.buffer.ByteBufAllocator;

import org.neo4j.io.bufferpool.ByteBufferManger;

public class CentralBufferMangerHolder
{
    public static final CentralBufferMangerHolder EMPTY = new CentralBufferMangerHolder( null, null );

    private final ByteBufAllocator nettyBufferAllocator;
    private final ByteBufferManger byteBufferManger;

    public CentralBufferMangerHolder( ByteBufAllocator nettyBufferAllocator, ByteBufferManger byteBufferManger )
    {
        this.nettyBufferAllocator = nettyBufferAllocator;
        this.byteBufferManger = byteBufferManger;
    }

    /**
     * Gets an instance of Netty's {@link ByteBufAllocator} if there is an instance
     * managed centrally for the entire DBMS or {@code null } otherwise.
     */
    public ByteBufAllocator getNettyBufferAllocator()
    {
        return nettyBufferAllocator;
    }

    /**
     * Gets an instance of {@link ByteBufferManger} if there is an instance
     * managed centrally for the entire DBMS or {@code null } otherwise.
     */
    public ByteBufferManger getByteBufferManger()
    {
        return byteBufferManger;
    }
}
