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
package org.neo4j.causalclustering.core.consensus.protocol.v2;

import io.netty.channel.Channel;
import io.netty.channel.ChannelInboundHandler;

import java.time.Clock;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.messaging.marshalling.v2.ContentTypeProtocol;
import org.neo4j.causalclustering.messaging.marshalling.v2.decoding.ContentTypeDispatcher;
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

public class RaftProtocolServerInstallerV2 implements ProtocolInstaller<Orientation.Server>
{
    private static final Protocol.ApplicationProtocols APPLICATION_PROTOCOL = Protocol.ApplicationProtocols.RAFT_2;
    private final LogProvider logProvider;

    public static class Factory extends ProtocolInstaller.Factory<Orientation.Server,RaftProtocolServerInstallerV2>
    {
        public Factory( ChannelInboundHandler raftMessageHandler, NettyPipelineBuilderFactory pipelineBuilderFactory, LogProvider logProvider )
        {
            super( APPLICATION_PROTOCOL, modifiers -> new RaftProtocolServerInstallerV2( raftMessageHandler, pipelineBuilderFactory, modifiers, logProvider ) );
        }
    }

    private final ChannelInboundHandler raftMessageHandler;
    private final NettyPipelineBuilderFactory pipelineBuilderFactory;
    private final List<ModifierProtocolInstaller<Orientation.Server>> modifiers;
    private final Log log;

    public RaftProtocolServerInstallerV2( ChannelInboundHandler raftMessageHandler, NettyPipelineBuilderFactory pipelineBuilderFactory,
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
        DecodingDispatcher decodingDispatcher = new DecodingDispatcher( contentTypeProtocol, logProvider );
        pipelineBuilderFactory
                .server( channel, log )
                .modify( modifiers )
                .addFraming()
                .onClose( decodingDispatcher::close )
                .add( "raft_content_type_dispatcher", new ContentTypeDispatcher( contentTypeProtocol ) )
                .add( "raft_component_decoder", decodingDispatcher )
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
