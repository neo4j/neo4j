/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.core.consensus;

import io.netty.channel.Channel;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.neo4j.causalclustering.messaging.CoreReplicatedContentMarshal;
import org.neo4j.causalclustering.messaging.marshalling.RaftMessageEncoder;
import org.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.causalclustering.protocol.ProtocolInstaller;
import org.neo4j.causalclustering.protocol.ProtocolInstaller.Orientation;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class RaftProtocolClientInstaller implements ProtocolInstaller<Orientation.Client>
{
    private static final Protocol.ApplicationProtocols APPLICATION_PROTOCOL = Protocol.ApplicationProtocols.RAFT_1;

    public static class Factory extends ProtocolInstaller.Factory<Orientation.Client, RaftProtocolClientInstaller>
    {
        public Factory( NettyPipelineBuilderFactory clientPipelineBuilderFactory, LogProvider logProvider )
        {
            super( APPLICATION_PROTOCOL,
                    modifiers -> new RaftProtocolClientInstaller( clientPipelineBuilderFactory, modifiers, logProvider ) );
        }
    }

    private final List<ModifierProtocolInstaller<Orientation.Client>> modifiers;
    private final Log log;
    private final NettyPipelineBuilderFactory clientPipelineBuilderFactory;

    public RaftProtocolClientInstaller( NettyPipelineBuilderFactory clientPipelineBuilderFactory, List<ModifierProtocolInstaller<Orientation.Client>> modifiers,
            LogProvider logProvider )
    {
        this.modifiers = modifiers;
        this.log = logProvider.getLog( getClass() );
        this.clientPipelineBuilderFactory = clientPipelineBuilderFactory;
    }

    @Override
    public void install( Channel channel ) throws Exception
    {
        clientPipelineBuilderFactory.client( channel, log )
                .modify( modifiers )
                .addFraming()
                .add( "raft_encoder", new RaftMessageEncoder( new CoreReplicatedContentMarshal() ) )
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
        return modifiers.stream()
                .map( ModifierProtocolInstaller::protocols )
                .collect( Collectors.toList() );
    }
}
