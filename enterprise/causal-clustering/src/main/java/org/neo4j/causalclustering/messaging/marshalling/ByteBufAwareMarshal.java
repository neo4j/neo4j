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

import java.io.IOException;

import org.neo4j.causalclustering.messaging.NetworkFlushableChannelNetty4;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.storageengine.api.WritableChannel;

public interface ByteBufAwareMarshal extends Marshal
{
    /** Provides a {@link ByteBufAwareMarshal} with unknown length that uses the same consumer for encode and marshal. May override buffer allocation size.
     * @param channelConsumer used by both encode and marshal to serialize the object.
     * @return a {@link ByteBufAwareMarshal} that encodes all the content at once.
     */
    static ByteBufAwareMarshal simple( ThrowingConsumer<WritableChannel,IOException> channelConsumer )
    {
        return new ByteBufAwareMarshal()
        {
            private boolean consumed;

            @Override
            public boolean encode( ByteBuf byteBuf ) throws IOException
            {
                if ( consumed )
                {
                    return false;
                }
                marshal( new NetworkFlushableChannelNetty4( byteBuf ) );
                consumed = true;
                return false;
            }

            @Override
            public int length()
            {
                return -1;
            }

            @Override
            public void marshal( WritableChannel channel ) throws IOException
            {
                channelConsumer.accept( channel );
            }
        };
    }

    /**
     * Writes to ByteBuf until there is no more left to write. Should write equal to or less the amount of writable bytes in the buffer.
     *
     * @param byteBuf where data will be written
     * @return false if there is no more data left to write after this call.
     */
    boolean encode( ByteBuf byteBuf ) throws IOException;

    /** This is used by the decoding side to allocate the byte buffer for the de-serialized object.
     * Only needed if the de-serialized object may be very large and if it needs to be fully written to memory.
     * @return the total bytes of the serialized object.
     */
    default int length()
    {
        return -1;
    }
}
