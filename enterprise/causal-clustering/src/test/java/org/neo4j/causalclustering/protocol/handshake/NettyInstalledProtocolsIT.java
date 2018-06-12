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
package org.neo4j.causalclustering.protocol.handshake;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandler;
import io.netty.channel.ChannelOption;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.core.consensus.RaftMessages;
import org.neo4j.causalclustering.core.consensus.protocol.v1.RaftProtocolClientInstallerV1;
import org.neo4j.causalclustering.core.consensus.protocol.v1.RaftProtocolServerInstallerV1;
import org.neo4j.causalclustering.core.consensus.protocol.v2.RaftProtocolClientInstallerV2;
import org.neo4j.causalclustering.core.consensus.protocol.v2.RaftProtocolServerInstallerV2;
import org.neo4j.causalclustering.handlers.VoidPipelineWrapperFactory;
import org.neo4j.causalclustering.identity.ClusterId;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.causalclustering.protocol.ModifierProtocolInstaller;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.causalclustering.protocol.Protocol.ModifierProtocol;
import org.neo4j.causalclustering.protocol.Protocol.ModifierProtocols;
import org.neo4j.causalclustering.protocol.ProtocolInstaller;
import org.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.FormattedLogProvider;
import org.neo4j.logging.LogProvider;
import org.neo4j.ports.allocation.PortAuthority;
import org.neo4j.stream.Streams;

import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.contains;
import static org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocolCategory.RAFT;
import static org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols.RAFT_1;
import static org.neo4j.causalclustering.protocol.Protocol.ApplicationProtocols.RAFT_2;
import static org.neo4j.causalclustering.protocol.Protocol.ModifierProtocolCategory.COMPRESSION;
import static org.neo4j.test.assertion.Assert.assertEventually;

@RunWith( Parameterized.class )
public class NettyInstalledProtocolsIT
{
    private Parameters parameters;

    public NettyInstalledProtocolsIT( Parameters parameters )
    {
        this.parameters = parameters;
    }

    private static final int TIMEOUT_SECONDS = 10;
    private static final LogProvider logProvider = FormattedLogProvider.toOutputStream( System.out );

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Parameters> data()
    {
        Stream<Optional<ModifierProtocol>> noModifierProtocols = Stream.of( Optional.empty() );
        Stream<Optional<ModifierProtocol>> individualModifierProtocols = Stream.of( ModifierProtocols.values() ).map( Optional::of );

        return Stream
                .concat( noModifierProtocols, individualModifierProtocols )
                .flatMap( protocol -> Stream.of( raft1WithCompressionModifier( protocol ), raft2WithCompressionModifiers( protocol ) ) )
                .collect( Collectors.toList() );
    }

    @SuppressWarnings( "OptionalUsedAsFieldOrParameterType" )
    private static Parameters raft1WithCompressionModifier( Optional<ModifierProtocol> protocol )
    {
        List<String> versions = Streams.ofOptional( protocol ).map( Protocol::implementation ).collect( Collectors.toList() );
        return new Parameters( "Raft 1, modifiers: " + protocol, new ApplicationSupportedProtocols( RAFT, singletonList( RAFT_1.implementation() ) ),
                singletonList( new ModifierSupportedProtocols( COMPRESSION, versions ) ) );
    }

    @SuppressWarnings( "OptionalUsedAsFieldOrParameterType" )
    private static Parameters raft2WithCompressionModifiers( Optional<ModifierProtocol> protocol )
    {
        List<String> versions = Streams.ofOptional( protocol ).map( Protocol::implementation ).collect( Collectors.toList() );
        return new Parameters( "Raft 2, modifiers: " + protocol, new ApplicationSupportedProtocols( RAFT, singletonList( RAFT_2.implementation() ) ),
                singletonList( new ModifierSupportedProtocols( COMPRESSION, versions ) ) );
    }

    @Test
    public void shouldSuccessfullySendAndReceiveAMessage() throws Throwable
    {
        // given
        RaftMessages.Heartbeat raftMessage = new RaftMessages.Heartbeat( new MemberId( UUID.randomUUID() ), 1, 2, 3 );
        RaftMessages.ClusterIdAwareMessage<RaftMessages.Heartbeat> networkMessage =
                RaftMessages.ClusterIdAwareMessage.of( new ClusterId( UUID.randomUUID() ), raftMessage );

        // when
        client.send( networkMessage ).syncUninterruptibly();

        // then
        assertEventually(
                messages -> String.format( "Received messages %s should contain message decorating %s", messages, raftMessage ),
                () -> server.received(),
                contains( messageMatches( networkMessage ) ), TIMEOUT_SECONDS, SECONDS );
    }

    private Server server;
    private Client client;

    @Before
    public void setUp()
    {
        ApplicationProtocolRepository applicationProtocolRepository =
                new ApplicationProtocolRepository( Protocol.ApplicationProtocols.values(), parameters.applicationSupportedProtocol );
        ModifierProtocolRepository modifierProtocolRepository =
                new ModifierProtocolRepository( ModifierProtocols.values(), parameters.modifierSupportedProtocols );

        NettyPipelineBuilderFactory serverPipelineBuilderFactory = new NettyPipelineBuilderFactory( VoidPipelineWrapperFactory.VOID_WRAPPER );
        NettyPipelineBuilderFactory clientPipelineBuilderFactory = new NettyPipelineBuilderFactory( VoidPipelineWrapperFactory.VOID_WRAPPER );

        server = new Server( serverPipelineBuilderFactory );
        server.start( applicationProtocolRepository, modifierProtocolRepository );

        Config config = Config.builder().withSetting( CausalClusteringSettings.handshake_timeout, TIMEOUT_SECONDS + "s" ).build();

        client = new Client( applicationProtocolRepository, modifierProtocolRepository, clientPipelineBuilderFactory, config );

        client.connect( server.port() );
    }

    @After
    public void tearDown()
    {
        client.disconnect();
        server.stop();
    }

    private static class Parameters
    {
        final String name;
        final ApplicationSupportedProtocols applicationSupportedProtocol;
        final Collection<ModifierSupportedProtocols> modifierSupportedProtocols;

        Parameters( String name, ApplicationSupportedProtocols applicationSupportedProtocol,
                Collection<ModifierSupportedProtocols> modifierSupportedProtocols )
        {
            this.name = name;
            this.applicationSupportedProtocol = applicationSupportedProtocol;
            this.modifierSupportedProtocols = modifierSupportedProtocols;
        }

        @Override
        public String toString()
        {
            return name;
        }
    }

    static class Server
    {
        private Channel channel;
        private NioEventLoopGroup eventLoopGroup;
        private final List<Object> received = new CopyOnWriteArrayList<>();
        private NettyPipelineBuilderFactory pipelineBuilderFactory;

        ChannelInboundHandler nettyHandler = new SimpleChannelInboundHandler<Object>()
        {
            @Override
            protected void channelRead0( ChannelHandlerContext ctx, Object msg )
            {
                received.add( msg );
            }
        };

        Server( NettyPipelineBuilderFactory pipelineBuilderFactory )
        {
            this.pipelineBuilderFactory = pipelineBuilderFactory;
        }

        void start( final ApplicationProtocolRepository applicationProtocolRepository, final ModifierProtocolRepository modifierProtocolRepository )
        {
            RaftProtocolServerInstallerV2.Factory raftFactoryV2 =
                    new RaftProtocolServerInstallerV2.Factory( nettyHandler, pipelineBuilderFactory, logProvider );
            RaftProtocolServerInstallerV1.Factory raftFactoryV1 =
                    new RaftProtocolServerInstallerV1.Factory( nettyHandler, pipelineBuilderFactory,
                            logProvider );
            ProtocolInstallerRepository<ProtocolInstaller.Orientation.Server> protocolInstallerRepository =
                    new ProtocolInstallerRepository<>( Arrays.asList( raftFactoryV1, raftFactoryV2 ), ModifierProtocolInstaller.allServerInstallers );

            eventLoopGroup = new NioEventLoopGroup();
            ServerBootstrap bootstrap = new ServerBootstrap().group( eventLoopGroup )
                    .channel( NioServerSocketChannel.class )
                    .option( ChannelOption.SO_REUSEADDR, true )
                    .localAddress( PortAuthority.allocatePort() )
                    .childHandler( new HandshakeServerInitializer( applicationProtocolRepository, modifierProtocolRepository,
                            protocolInstallerRepository, pipelineBuilderFactory, logProvider ).asChannelInitializer() );

            channel = bootstrap.bind().syncUninterruptibly().channel();
        }

        void stop()
        {
            channel.close().syncUninterruptibly();
            eventLoopGroup.shutdownGracefully( 0, TIMEOUT_SECONDS, SECONDS );
        }

        int port()
        {
            return ((InetSocketAddress) channel.localAddress()).getPort();
        }

        public Collection<Object> received()
        {
            return received;
        }
    }

    static class Client
    {
        private Bootstrap bootstrap;
        private NioEventLoopGroup eventLoopGroup;
        private Channel channel;
        private HandshakeClientInitializer handshakeClientInitializer;

        Client( ApplicationProtocolRepository applicationProtocolRepository, ModifierProtocolRepository modifierProtocolRepository,
                NettyPipelineBuilderFactory pipelineBuilderFactory, Config config )
        {
            RaftProtocolClientInstallerV2.Factory raftFactoryV2 = new RaftProtocolClientInstallerV2.Factory( pipelineBuilderFactory, logProvider );
            RaftProtocolClientInstallerV1.Factory raftFactoryV1 =
                    new RaftProtocolClientInstallerV1.Factory( pipelineBuilderFactory, logProvider );
            ProtocolInstallerRepository<ProtocolInstaller.Orientation.Client> protocolInstallerRepository =
                    new ProtocolInstallerRepository<>( Arrays.asList( raftFactoryV1, raftFactoryV2 ), ModifierProtocolInstaller.allClientInstallers );
            eventLoopGroup = new NioEventLoopGroup();
            Duration handshakeTimeout = config.get( CausalClusteringSettings.handshake_timeout );
            handshakeClientInitializer = new HandshakeClientInitializer( applicationProtocolRepository, modifierProtocolRepository,
                    protocolInstallerRepository, pipelineBuilderFactory, handshakeTimeout, logProvider, logProvider );
            bootstrap = new Bootstrap().group( eventLoopGroup ).channel( NioSocketChannel.class ).handler( handshakeClientInitializer );
        }

        @SuppressWarnings( "SameParameterValue" )
        void connect( int port )
        {
            ChannelFuture channelFuture = bootstrap.connect( "localhost", port ).syncUninterruptibly();
            channel = channelFuture.channel();
        }

        void disconnect()
        {
            if ( channel != null )
            {
                channel.close().syncUninterruptibly();
                eventLoopGroup.shutdownGracefully( 0, TIMEOUT_SECONDS, SECONDS ).syncUninterruptibly();
            }
        }

        ChannelFuture send( Object message )
        {
            return channel.writeAndFlush( message );
        }
    }

    private Matcher<Object> messageMatches( RaftMessages.ClusterIdAwareMessage<? extends RaftMessages.RaftMessage> expected )
    {
        return new MessageMatcher( expected );
    }

    class MessageMatcher extends BaseMatcher<Object>
    {
        private final RaftMessages.ClusterIdAwareMessage<? extends RaftMessages.RaftMessage> expected;

        MessageMatcher( RaftMessages.ClusterIdAwareMessage<? extends RaftMessages.RaftMessage> expected )
        {
            this.expected = expected;
        }

        @Override
        public boolean matches( Object item )
        {
            if ( item instanceof RaftMessages.ClusterIdAwareMessage<?> )
            {
                RaftMessages.ClusterIdAwareMessage<?> message = (RaftMessages.ClusterIdAwareMessage<?>) item;
                return message.clusterId().equals( expected.clusterId() ) && message.message().equals( expected.message() );
            }
            else
            {
                return false;
            }
        }

        @Override
        public void describeTo( Description description )
        {
            description.appendText( "Cluster ID " ).appendValue( expected.clusterId() ).appendText( " message " ).appendValue( expected.message() );
        }
    }
}
