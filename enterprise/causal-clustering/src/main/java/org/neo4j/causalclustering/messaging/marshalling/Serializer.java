/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.causalclustering.messaging.marshalling;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import org.neo4j.causalclustering.messaging.NetworkFlushableChannelNetty4;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.storageengine.api.WritableChannel;

public interface Serializer extends Marshal
{
    /** May override buffer allocation size.
     * @param channelConsumer
     * @return a simple serializer that encodes all the content at once.
     */
    static Serializer simple( ThrowingConsumer<WritableChannel,IOException> channelConsumer )
    {
        return new Serializer()
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
}
