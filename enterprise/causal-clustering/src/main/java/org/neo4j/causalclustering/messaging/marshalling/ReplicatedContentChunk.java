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
import io.netty.buffer.DefaultByteBufHolder;

public class ReplicatedContentChunk extends DefaultByteBufHolder
{
    private final byte contentType;
    private final boolean isLast;

    private ReplicatedContentChunk( byte contentType, boolean isLast, ByteBuf data )
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

    public static ReplicatedContentChunk deSerialize( ByteBuf in )
    {
        byte txContentType = in.readByte();
        boolean isLast = in.readBoolean();
        return new ReplicatedContentChunk( txContentType, isLast, in.readSlice( in.readableBytes() ) );
    }
}
