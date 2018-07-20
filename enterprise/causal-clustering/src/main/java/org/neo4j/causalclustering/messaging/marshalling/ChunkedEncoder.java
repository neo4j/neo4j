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
package org.neo4j.causalclustering.messaging.marshalling;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;

import java.io.IOException;

import org.neo4j.causalclustering.messaging.NetworkFlushableChannelNetty4;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.storageengine.api.WritableChannel;

public interface ChunkedEncoder extends Marshal
{
    /**
     * Provides a {@link ChunkedEncoder} with unknown length that uses the same consumer for encode and marshal.
     * This class methods is not idempotent because of the {@code isEndOfInput} and should only encode once.
     *
     * @param channelConsumer used by both encode and marshal to serialize the object.
     * @return a {@link ChunkedEncoder} that encodes all the content at once.
     */
    static ChunkedEncoder single( ThrowingConsumer<WritableChannel,IOException> channelConsumer )
    {
        return new ChunkedEncoder()
        {
            private boolean encoded;

            @Override
            public ByteBuf encodeChunk( ByteBufAllocator allocator ) throws IOException
            {
                if ( encoded )
                {
                    return null;
                }

                ByteBuf buffer = allocator.buffer();
                try
                {
                    marshal( new NetworkFlushableChannelNetty4( buffer ) );
                    encoded = true;
                    return buffer;
                }
                catch ( Throwable t )
                {
                    buffer.release();
                    throw t;
                }
            }

            @Override
            public boolean isEndOfInput()
            {
                return encoded;
            }

            @Override
            public void marshal( WritableChannel channel ) throws IOException
            {
                channelConsumer.accept( channel );
            }
        };
    }

    /**
     * Uses the allocator to return byte buffs, will be called until isEndOfInput is true.
     *
     * @param allocator for allocating chunks
     * @return ByteBuf containg the chunk or {@code null} if there is currently no chunk available. Null does not mean that this is completed,
     * that is decided byt {@code isEndOfInput}
     */
    ByteBuf encodeChunk( ByteBufAllocator allocator ) throws IOException;

    /**
     * @return If all chunks have been encoed and returned
     */
    boolean isEndOfInput();
}
