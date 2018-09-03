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
import io.netty.buffer.ByteBufInputStream;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.DefaultByteBufHolder;

import java.io.IOException;

import org.neo4j.storageengine.api.WritableChannel;

public class ByteBufMarshal extends DefaultByteBufHolder implements ChunkedEncoder
{
    private static final int CHUNK_SIZE = 1024;
    private boolean encoded;

    public ByteBufMarshal( ByteBuf content )
    {
        super( content );
    }

    @Override
    public void marshal( WritableChannel channel ) throws IOException
    {
        channel.putInt( content().writerIndex() );
        if ( content().hasArray() )
        {
            channel.put( content().array(), content().writerIndex() );
        }
        else
        {
            ByteBufInputStream byteBufInputStream = new ByteBufInputStream( content() );
            byte[] bytes = new byte[CHUNK_SIZE];
            int read;
            while ( ( read = byteBufInputStream.read( bytes ) ) != -1 )
            {
                channel.put( bytes, read );
            }
        }
    }

    @Override
    public ByteBuf encodeChunk( ByteBufAllocator allocator )
    {
        if ( encoded )
        {
            return null;
        }
        encoded = true;
        ByteBuf lengthPrepend = allocator.buffer( 4, 4 );
        lengthPrepend.writeInt( content().writerIndex() );
        return new CompositeByteBuf( allocator, true, 2 ).addComponent( true, lengthPrepend ).addComponent( true, content() );
    }

    @Override
    public boolean isEndOfInput()
    {
        return encoded;
    }
}
