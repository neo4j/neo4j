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
package org.neo4j.causalclustering.messaging;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.membership.MemberIdSet;
import org.neo4j.causalclustering.core.consensus.protocol.v1.RaftProtocolClientInstallerV1;
import org.neo4j.causalclustering.core.consensus.protocol.v1.RaftProtocolServerInstallerV1;
import org.neo4j.causalclustering.core.consensus.protocol.v2.RaftProtocolClientInstallerV2;
import org.neo4j.causalclustering.core.consensus.protocol.v2.RaftProtocolServerInstallerV2;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.net.Server;
import org.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols;
import org.neo4j.causalclustering.protocol.Protocol.ModifierProtocols;
import org.neo4j.causalclustering.protocol.ProtocolInstaller;
import org.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import org.neo4j.causalclustering.protocol.handshake.ApplicationProtocolRepository;
import org.neo4j.causalclustering.protocol.handshake.ApplicationSupportedProtocols;
import org.neo4j.causalclustering.protocol.handshake.HandshakeClientInitializer;
import org.neo4j.causalclustering.protocol.handshake.HandshakeServerInitializer;
import org.neo4j.causalclustering.protocol.handshake.ModifierProtocolRepository;
import org.neo4j.causalclustering.protocol.handshake.ModifierSupportedProtocols;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.ListenSocketAddress;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.ports.allocation.PortAuthority;

import static java.util.Collections.emptyList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;
import static org.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory.VOID_WRAPPER;
import static org.neo4j.helpers.collection.Iterators.asSet;

@RunWith( Parameterized.class )
public class SenderServiceIT
{
    private final LogProvider logProvider = NullLogProvider.getInstance();

    private final ApplicationSupportedProtocols supportedApplicationProtocol = new ApplicationSupportedProtocols( Protocol.ApplicationProtocolCategory.RAFT,
            Arrays.asList( ApplicationProtocols.RAFT_1.implementation(), ApplicationProtocols.RAFT_2.implementation() ) );
    private final Collection<ModifierSupportedProtocols> supportedModifierProtocols = emptyList();

    private final ApplicationProtocolRepository applicationProtocolRepository =
            new ApplicationProtocolRepository( ApplicationProtocols.values(), supportedApplicationProtocol );
    private final ModifierProtocolRepository modifierProtocolRepository =
            new ModifierProtocolRepository( ModifierProtocols.values(), supportedModifierProtocols );

    @Parameterized.Parameter
    public boolean blocking;

    @Parameterized.Parameter( 1 )
    public ApplicationProtocols clientProtocol;

    @Parameterized.Parameters( name = "blocking={0} protocol={1}" )
    public static Iterable<Object[]> params()
    {
        return clientRepositories().stream().flatMap( r -> Stream.of( new Object[]{true, r}, new Object[]{false, r} ) ).collect( Collectors.toList() );
    }

    private static Collection<ApplicationProtocols> clientRepositories()
    {
        return Arrays.asList( ApplicationProtocols.RAFT_1, ApplicationProtocols.RAFT_2 );
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
        Server raftServer = raftServer( nettyHandler, port );
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

    private Server raftServer( ChannelInboundHandler nettyHandler, int port )
    {
        NettyPipelineBuilderFactory pipelineFactory = new NettyPipelineBuilderFactory( VOID_WRAPPER );

        RaftProtocolServerInstallerV1.Factory factoryV1 = new RaftProtocolServerInstallerV1.Factory( nettyHandler, pipelineFactory, logProvider );
        RaftProtocolServerInstallerV2.Factory factoryV2 =
                new RaftProtocolServerInstallerV2.Factory( nettyHandler, pipelineFactory, logProvider );
        ProtocolInstallerRepository<ProtocolInstaller.Orientation.Server> installer =
                new ProtocolInstallerRepository<>( Arrays.asList( factoryV1, factoryV2 ), ModifierProtocolInstaller.allServerInstallers );

        HandshakeServerInitializer channelInitializer = new HandshakeServerInitializer( applicationProtocolRepository, modifierProtocolRepository,
                installer, pipelineFactory, logProvider );

        ListenSocketAddress listenAddress = new ListenSocketAddress( "localhost", port );
        return new Server( channelInitializer, null, logProvider, logProvider, listenAddress, "raft-server" );
    }

    private SenderService raftSender()
    {
        NettyPipelineBuilderFactory pipelineFactory = new NettyPipelineBuilderFactory( VOID_WRAPPER );

        RaftProtocolClientInstallerV1.Factory factoryV1 = new RaftProtocolClientInstallerV1.Factory( pipelineFactory, logProvider );
        RaftProtocolClientInstallerV2.Factory factoryV2 =
                new RaftProtocolClientInstallerV2.Factory( pipelineFactory, logProvider );
        ProtocolInstallerRepository<ProtocolInstaller.Orientation.Client> protocolInstaller =
                new ProtocolInstallerRepository<>( Arrays.asList( factoryV1, factoryV2 ), ModifierProtocolInstaller.allClientInstallers );

        HandshakeClientInitializer channelInitializer = new HandshakeClientInitializer( clientRepository(),
                modifierProtocolRepository,
                protocolInstaller,
                pipelineFactory,
                Duration.ofSeconds(5),
                logProvider,
                logProvider );

        return new SenderService( channelInitializer, logProvider );
    }

    private ApplicationProtocolRepository clientRepository()
    {
        return new ApplicationProtocolRepository( new ApplicationProtocols[]{ApplicationProtocols.RAFT_2},
                new ApplicationSupportedProtocols( Protocol.ApplicationProtocolCategory.RAFT, emptyList() ) );
    }
}
