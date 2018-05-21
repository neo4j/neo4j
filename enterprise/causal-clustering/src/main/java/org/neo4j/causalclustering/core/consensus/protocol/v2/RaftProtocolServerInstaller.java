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
package org.neo4j.causalclustering.core.consensus.protocol.v2;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandler;

import java.time.Clock;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.messaging.marshalling.v2.decoding.ContentTypeDispatcher;
import org.neo4j.causalclustering.messaging.marshalling.v2.ContentTypeProtocol;
import org.neo4j.causalclustering.messaging.marshalling.v2.decoding.DecodingDispatcher;
import org.neo4j.causalclustering.messaging.marshalling.v2.decoding.RaftMessageComposer;
import org.neo4j.causalclustering.messaging.marshalling.v2.decoding.ReplicatedContentDecoder;
import org.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.causalclustering.protocol.ProtocolInstaller;
import org.neo4j.causalclustering.protocol.ProtocolInstaller.Orientation;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class RaftProtocolServerInstaller implements ProtocolInstaller<Orientation.Server>
{
    private static final Protocol.ApplicationProtocols APPLICATION_PROTOCOL = Protocol.ApplicationProtocols.RAFT_2;
    private final LogProvider logProvider;

    public static class Factory extends ProtocolInstaller.Factory<Orientation.Server,RaftProtocolServerInstaller>
    {
        public Factory( ChannelInboundHandler raftMessageHandler, NettyPipelineBuilderFactory pipelineBuilderFactory, LogProvider logProvider )
        {
            super( APPLICATION_PROTOCOL, modifiers -> new RaftProtocolServerInstaller( raftMessageHandler, pipelineBuilderFactory, modifiers, logProvider ) );
        }
    }

    private final ChannelInboundHandler raftMessageHandler;
    private final NettyPipelineBuilderFactory pipelineBuilderFactory;
    private final List<ModifierProtocolInstaller<Orientation.Server>> modifiers;
    private final Log log;

    public RaftProtocolServerInstaller( ChannelInboundHandler raftMessageHandler, NettyPipelineBuilderFactory pipelineBuilderFactory,
            List<ModifierProtocolInstaller<Orientation.Server>> modifiers, LogProvider logProvider )
    {
        this.raftMessageHandler = raftMessageHandler;
        this.pipelineBuilderFactory = pipelineBuilderFactory;
        this.modifiers = modifiers;
        this.logProvider = logProvider;
        this.log = this.logProvider.getLog( getClass() );
    }

    @Override
    public void install( Channel channel ) throws Exception
    {

        ContentTypeProtocol contentTypeProtocol = new ContentTypeProtocol();
        pipelineBuilderFactory
                .server( channel, log )
                .modify( modifiers )
                .addFraming()
                .add( "raft_content_type_dispatcher", new ContentTypeDispatcher( contentTypeProtocol ) )
                .add( "raft_component_decoder", new DecodingDispatcher( contentTypeProtocol, logProvider ) )
                .add( "raft_content_decoder", new ReplicatedContentDecoder( contentTypeProtocol ) )
                .add( "raft_message_composer", new RaftMessageComposer( Clock.systemUTC() ) )
                .add( "raft_handler", raftMessageHandler )
                .install();
    }

    @Override
    public Protocol.ApplicationProtocol applicationProtocol()
    {
        return APPLICATION_PROTOCOL;
    }

    @Override
    public Collection<Collection<Protocol.ModifierProtocol>> modifiers()
    {
        return modifiers.stream().map( ModifierProtocolInstaller::protocols ).collect( Collectors.toList() );
    }
}
