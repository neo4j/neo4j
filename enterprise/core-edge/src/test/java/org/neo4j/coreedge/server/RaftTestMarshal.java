/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.server;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.coreedge.raft.state.ByteBufferMarshal;
import org.neo4j.coreedge.raft.state.ChannelMarshal;
import org.neo4j.storageengine.api.ReadableChannel;
import org.neo4j.storageengine.api.WritableChannel;

public class RaftTestMarshal implements ByteBufferMarshal<RaftTestMember>, ChannelMarshal<RaftTestMember>
{
    @Override
    public void marshal( RaftTestMember raftTestMember, ByteBuffer target )
    {
        target.putLong( raftTestMember.getId() );
    }

    @Override
    public RaftTestMember unmarshal( ByteBuffer source )
    {
        return RaftTestMember.member( source.getLong() );
    }

    @Override
    public void marshal( RaftTestMember target, WritableChannel channel ) throws IOException
    {
        channel.putLong( target.getId() );
    }

    @Override
    public RaftTestMember unmarshal( ReadableChannel source ) throws IOException
    {
        return RaftTestMember.member( source.getLong() );
    }
}
