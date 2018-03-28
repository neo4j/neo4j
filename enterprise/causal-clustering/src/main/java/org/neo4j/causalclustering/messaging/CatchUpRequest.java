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
package org.neo4j.causalclustering.messaging;

import io.netty.buffer.ByteBuf;

import java.io.IOException;
import java.util.Objects;

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

    String messageId()
    {
        return id;
    }
}
