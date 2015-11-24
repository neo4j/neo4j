/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.raft.replication;

import java.nio.ByteBuffer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import org.neo4j.coreedge.raft.net.CoreReplicatedContentMarshal;

public class RaftContentSerializer implements Serializer
{
    @Override
    public ByteBuffer serialize( ReplicatedContent content ) throws MarshallingException
    {
        ByteBuf buffer = Unpooled.buffer();
        new CoreReplicatedContentMarshal().serialize( content, buffer );
        return buffer.nioBuffer();
    }

    @Override
    public ReplicatedContent deserialize( ByteBuffer buffer ) throws MarshallingException
    {
        return new CoreReplicatedContentMarshal().deserialize( Unpooled.wrappedBuffer( buffer ) );
    }
}
