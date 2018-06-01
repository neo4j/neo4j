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

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.neo4j.storageengine.api.WritableChannel;

public class ByteArrayByteBufAwareMarshal implements ByteBufAwareMarshal
{
    private final byte[] content;
    private final ByteArrayInputStream inputStream;

    public ByteArrayByteBufAwareMarshal( byte[] content )
    {
        inputStream = new ByteArrayInputStream( content );
        this.content = content;
    }

    @Override
    public boolean encode( ByteBuf byteBuf ) throws IOException
    {
        if ( inputStream.available() == content.length )
        {
            if ( !byteBuf.isWritable( 5 ) )
            {
                return true;
            }

            byteBuf.writeInt( content.length );
        }
        if ( !hasBytes() )
        {
            return false;
        }
        int toWrite = Math.min( inputStream.available(), byteBuf.writableBytes() );
        byteBuf.writeBytes( inputStream, toWrite );
        return hasBytes();
    }

    @Override
    public int length()
    {
        // initial int plus array length
        return content.length + 4;
    }

    private boolean hasBytes()
    {
        return inputStream.available() > 0;
    }

    @Override
    public void marshal( WritableChannel channel ) throws IOException
    {
        int length = content.length;
        channel.putInt( length );
        channel.put( content, length );
    }
}
