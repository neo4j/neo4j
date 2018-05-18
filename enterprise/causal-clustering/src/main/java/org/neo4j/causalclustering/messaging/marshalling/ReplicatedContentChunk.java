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
import io.netty.buffer.DefaultByteBufHolder;

public class ReplicatedContentChunk extends DefaultByteBufHolder
{
    private final byte contentType;
    private final boolean isLast;

    ReplicatedContentChunk( byte contentType, boolean isLast, ByteBuf data )
    {
        super( data );
        this.contentType = contentType;
        this.isLast = isLast;
    }

    public boolean isLast()
    {
        return isLast;
    }

    public byte contentType()
    {
        return contentType;
    }

    public void encode( ByteBuf out )
    {
        out.writeByte( contentType() );
        out.writeBoolean( isLast() );
        out.writeBytes( content() );
    }

    public static ReplicatedContentChunk deSerialize( ByteBuf in )
    {
        byte txContentType = in.readByte();
        boolean isLast = in.readBoolean();
        return new ReplicatedContentChunk( txContentType, isLast, in.readSlice( in.readableBytes() ) );
    }
}
