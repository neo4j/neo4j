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
package org.neo4j.coreedge.messaging;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;
import io.netty.handler.codec.LengthFieldPrepender;

import org.neo4j.coreedge.VersionPrepender;
import org.neo4j.coreedge.core.replication.ReplicatedContent;
import org.neo4j.coreedge.logging.ExceptionLoggingHandler;
import org.neo4j.coreedge.messaging.marshalling.ChannelMarshal;
import org.neo4j.coreedge.messaging.marshalling.RaftMessageEncoder;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class RaftChannelInitializer extends ChannelInitializer<SocketChannel>
{
    private final ChannelMarshal<ReplicatedContent> marshal;
    private final Log log;

    public RaftChannelInitializer( ChannelMarshal<ReplicatedContent> marshal, LogProvider logProvider )
    {
        this.marshal = marshal;
        log = logProvider.getLog( getClass() );
    }

    @Override
    protected void initChannel( SocketChannel ch ) throws Exception
    {
        ChannelPipeline pipeline = ch.pipeline();
        pipeline.addLast( "frameEncoder", new LengthFieldPrepender( 4 ) );
        pipeline.addLast( new VersionPrepender() );
        pipeline.addLast( "raftMessageEncoder", new RaftMessageEncoder( marshal ) );
        pipeline.addLast( new ExceptionLoggingHandler( log ) );
    }
}
