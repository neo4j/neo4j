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
package org.neo4j.causalclustering.messaging;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.UUID;
import java.util.concurrent.Semaphore;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.RaftProtocolClientInstaller;
import org.neo4j.causalclustering.core.consensus.RaftProtocolServerInstaller;
import org.neo4j.causalclustering.core.consensus.RaftServer;
import org.neo4j.causalclustering.core.consensus.membership.MemberIdSet;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols;
import org.neo4j.causalclustering.protocol.Protocol.ModifierProtocols;
import org.neo4j.causalclustering.protocol.ProtocolInstaller;
import org.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import org.neo4j.causalclustering.protocol.handshake.HandshakeClientInitializer;
import org.neo4j.causalclustering.protocol.handshake.HandshakeServerInitializer;
import org.neo4j.causalclustering.protocol.handshake.ProtocolRepository;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ports.allocation.PortAuthority;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.raft_listen_address;
import static org.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory.VOID_WRAPPER;
import static org.neo4j.helpers.collection.Iterators.asSet;

@RunWith( Parameterized.class )
public class SenderServiceIT
{
    private final LogProvider logProvider = NullLogProvider.getInstance();
    private final ProtocolRepository<Protocol.ApplicationProtocol> applicationProtocolRepository = new ProtocolRepository<>( ApplicationProtocols.values() );
    private final ProtocolRepository<Protocol.ModifierProtocol> modifierProtocolRepository = new ProtocolRepository<>( ModifierProtocols.values() );

    @Parameterized.Parameter
    public boolean blocking;

    @Parameterized.Parameters( name = "blocking={0}" )
    public static Iterable<Boolean> params()
    {
        return asSet( true, false );
    }

    @Test
    public void shouldSendAndReceive() throws Throwable
    {
        // given: raft server handler
        int port = PortAuthority.allocatePort();
        Semaphore messageReceived = new Semaphore( 0 );
        ChannelInboundHandler nettyHandler = new ChannelInboundHandlerAdapter()
        {
            @Override
            public void channelRead( ChannelHandlerContext ctx, Object msg )
            {
                messageReceived.release();
            }
        };
        RaftServer raftServer = raftServer( nettyHandler, port );
        raftServer.start();

        // given: raft messaging service
        SenderService sender = raftSender();
        sender.start();

        // when
        AdvertisedSocketAddress to = new AdvertisedSocketAddress( "localhost", port );
        MemberId memberId = new MemberId( UUID.randomUUID() );
        ClusterId clusterId = new ClusterId( UUID.randomUUID() );

        RaftMessages.NewEntry.Request newEntryMessage = new RaftMessages.NewEntry.Request( memberId, new MemberIdSet( asSet( memberId ) ) );
        RaftMessages.ClusterIdAwareMessage<?> message = RaftMessages.ClusterIdAwareMessage.of( clusterId, newEntryMessage );

        sender.send( to, message, blocking );

        // then
        assertTrue( messageReceived.tryAcquire( 15, SECONDS ) );

        // cleanup
        sender.stop();
        raftServer.stop();
    }

    private RaftServer raftServer( ChannelInboundHandler nettyHandler, int port )
    {
        NettyPipelineBuilderFactory pipelineFactory = new NettyPipelineBuilderFactory( VOID_WRAPPER );

        RaftProtocolServerInstaller.Factory raftProtocolServerInstaller = new RaftProtocolServerInstaller.Factory( nettyHandler, pipelineFactory, logProvider );
        ProtocolInstallerRepository<ProtocolInstaller.Orientation.Server> installer =
                new ProtocolInstallerRepository<>( singletonList( raftProtocolServerInstaller ), ModifierProtocolInstaller.allServerInstallers );

        HandshakeServerInitializer channelInitializer = new HandshakeServerInitializer( logProvider, applicationProtocolRepository, modifierProtocolRepository,
                Protocol.ApplicationProtocolIdentifier.RAFT, installer, pipelineFactory );

        Config config = Config.defaults( raft_listen_address, new HostnamePort( "localhost", port ).toString() );
        return new RaftServer( channelInitializer, config, logProvider, logProvider );
    }

    private SenderService raftSender()
    {
        NettyPipelineBuilderFactory pipelineFactory = new NettyPipelineBuilderFactory( VOID_WRAPPER );

        RaftProtocolClientInstaller.Factory raftProtocolClientInstaller = new RaftProtocolClientInstaller.Factory( pipelineFactory, logProvider );
        ProtocolInstallerRepository<ProtocolInstaller.Orientation.Client> protocolInstaller =
                new ProtocolInstallerRepository<>( singletonList( raftProtocolClientInstaller ), ModifierProtocolInstaller.allClientInstallers );

        HandshakeClientInitializer channelInitializer = new HandshakeClientInitializer(
                applicationProtocolRepository,
                Protocol.ApplicationProtocolIdentifier.RAFT,
                modifierProtocolRepository,
                asSet( Protocol.ModifierProtocolIdentifier.COMPRESSION ),
                protocolInstaller, pipelineFactory,
                Config.defaults(),
                logProvider );

        return new SenderService( channelInitializer, logProvider );
    }
}
