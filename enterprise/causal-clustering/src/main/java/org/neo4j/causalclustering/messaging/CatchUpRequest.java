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
package org.neo4j.causalclustering.messaging;

import io.netty.buffer.ByteBuf;

import java.io.IOException;

import org.neo4j.causalclustering.catchup.RequestMessageType;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;
import org.neo4j.string.UTF8;

public abstract class CatchUpRequest implements Message
{
    private final String id;

    public CatchUpRequest( String id )
    {
        this.id = id;
    }

    public static String decodeMessage( ReadableChannel readableChannel ) throws IOException
    {
        int length = readableChannel.getInt();
        byte[] bytes = new byte[length];
        readableChannel.get( bytes, length );
        return UTF8.decode( bytes );
    }

    public static String decodeMessage( ByteBuf byteBuf )
    {
        int length = byteBuf.readInt();
        byte[] bytes = new byte[length];
        byteBuf.readBytes( bytes );
        return UTF8.decode( bytes );
    }

    public void encodeMessage( WritableChannel channel ) throws IOException
    {
        int length = id.length();
        channel.putInt( length );
        channel.put( UTF8.encode( id ), length );
    }

    public abstract RequestMessageType messageType();

    public String messageId()
    {
        return id;
    }

    @Override
    public String toString()
    {
        return "CatchUpRequest{" + "id='" + id + '\'' + '}';
    }
}
