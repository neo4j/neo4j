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
package org.neo4j.coreedge.raft.net;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;

import org.neo4j.coreedge.raft.net.codecs.RaftMessageEncoder;
import org.neo4j.coreedge.raft.replication.ReplicatedContentMarshal;

public class RaftChannelInitializer extends ChannelInitializer<SocketChannel>
{
    private final ReplicatedContentMarshal<ByteBuf> marshal;

    public RaftChannelInitializer( ReplicatedContentMarshal<ByteBuf> marshal )
    {
        this.marshal = marshal;
    }

    @Override
    protected void initChannel( SocketChannel ch ) throws Exception
    {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast( "frameEncoder", new LengthFieldPrepender( 4 ) );
        pipeline.addLast( "raftMessageEncoder", new RaftMessageEncoder( marshal ) );
    }
}
