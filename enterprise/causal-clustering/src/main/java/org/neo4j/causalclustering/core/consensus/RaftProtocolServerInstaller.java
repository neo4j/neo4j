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
package org.neo4j.causalclustering.core.consensus;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandler;

import java.time.Clock;

import org.neo4j.causalclustering.messaging.CoreReplicatedContentMarshal;
import org.neo4j.causalclustering.messaging.marshalling.RaftMessageDecoder;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.causalclustering.protocol.ProtocolInstaller;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class RaftProtocolServerInstaller extends ProtocolInstaller<ProtocolInstaller.Orientation.Server>
{
    private final ChannelInboundHandler raftMessageHandler;
    private final NettyPipelineBuilderFactory pipelineBuilderFactory;
    private final Log log;

    public RaftProtocolServerInstaller( ChannelInboundHandler raftMessageHandler, NettyPipelineBuilderFactory pipelineBuilderFactory, LogProvider logProvider )
    {
        super( Protocol.Protocols.RAFT_1 );
        this.raftMessageHandler = raftMessageHandler;
        this.pipelineBuilderFactory = pipelineBuilderFactory;
        this.log = logProvider.getLog( getClass() );
    }

    @Override
    public void install( Channel channel ) throws Exception
    {
        pipelineBuilderFactory.create( channel, log )
                .addFraming()
                .add( "raft_decoder", new RaftMessageDecoder( new CoreReplicatedContentMarshal(), Clock.systemUTC() ) )
                .add( "raft_handler", raftMessageHandler )
                .install();
    }
}
