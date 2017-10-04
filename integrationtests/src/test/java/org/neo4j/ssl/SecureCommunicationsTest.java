/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.ssl;

import io.netty.bootstrap.Bootstrap;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslProvider;
import org.apache.commons.lang3.SystemUtils;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLException;

import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.configuration.ssl.SslPolicyConfig;
import org.neo4j.kernel.configuration.ssl.SslPolicyLoader;
import org.neo4j.kernel.configuration.ssl.SslSystemSettings;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.isOneOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;
import static org.junit.Assume.assumeTrue;
import static org.neo4j.ssl.SslResourceBuilder.caSignedKeyId;
import static org.neo4j.ssl.SslResourceBuilder.selfSignedKeyId;
import static org.neo4j.test.assertion.Assert.assertEventually;

/**
 * This test mainly tests the SslContextFactory when it comes to production
 * code and serves as baseline implementation for how servers should inject
 * SSL into the pipeline utilizing the a security defining context.
 */
public class SecureCommunicationsTest
{
    private static final int UNRELATED_ID = 5; // SslContextFactory requires us to trust something

    private static final byte[] REQUEST = {1, 2, 3, 4};
    private static final byte[] RESPONSE = {5, 6, 7, 8};

    @Rule
    public TestDirectory testDir = TestDirectory.testDirectory();

    @Rule
    public DefaultFileSystemRule fsRule = new DefaultFileSystemRule();

    private SecureServer server;
    private SecureClient client;
    private ByteBuf expected;

    @After
    public void cleanup()
    {
        if ( expected != null )
        {
            expected.release();
        }
        if ( client != null )
        {
            client.disconnect();
        }
        if ( server != null )
        {
            server.stop();
        }
    }

    @Test
    public void partiesWithMutualTrustShouldCommunicate() throws Exception
    {
        // given
        SslResource sslServerResource = selfSignedKeyId( 0 ).trustKeyId( 1 ).install( testDir.directory( "server" ) );
        SslResource sslClientResource = selfSignedKeyId( 1 ).trustKeyId( 0 ).install( testDir.directory( "client" ) );

        server = new SecureServer( makeSslContext( sslServerResource, true ) );

        server.start();
        client = new SecureClient( makeSslContext( sslClientResource, false ) );
        client.connect( server.port() );

        // when
        ByteBuf request = ByteBufAllocator.DEFAULT.buffer().writeBytes( REQUEST );
        client.channel.writeAndFlush( request );

        // then
        expected = ByteBufAllocator.DEFAULT.buffer().writeBytes( RESPONSE );
        client.clientInitializer.handshakeFuture.get();
        client.assertResponse( expected );
    }

    @Test
    public void partiesWithMutualTrustThroughCAShouldCommunicate() throws Exception
    {
        // given
        SslResource sslServerResource = caSignedKeyId( 0 ).trustSignedByCA().install( testDir.directory( "server" ) );
        SslResource sslClientResource = caSignedKeyId( 1 ).trustSignedByCA().install( testDir.directory( "client" ) );

        server = new SecureServer( makeSslContext( sslServerResource, true ) );

        server.start();
        client = new SecureClient( makeSslContext( sslClientResource, false ) );
        client.connect( server.port() );

        // when
        ByteBuf request = ByteBufAllocator.DEFAULT.buffer().writeBytes( REQUEST );
        client.channel.writeAndFlush( request );

        // then
        expected = ByteBufAllocator.DEFAULT.buffer().writeBytes( RESPONSE );
        client.clientInitializer.handshakeFuture.get();
        client.assertResponse( expected );
    }

    @Test
    public void serverShouldNotCommunicateWithUntrustedClient() throws Exception
    {
        // given

        SslResource sslClientResource = selfSignedKeyId( 1 ).trustKeyId( 0 ).install( testDir.directory( "client" ) );
        SslResource sslServerResource = selfSignedKeyId( 0 ).trustKeyId( UNRELATED_ID ).install( testDir.directory( "server" ) );

        server = new SecureServer( makeSslContext( sslServerResource, true ) );

        server.start();
        client = new SecureClient( makeSslContext( sslClientResource, false ) );
        client.connect( server.port() );

        try
        {
            // when
            client.clientInitializer.handshakeFuture.get();
            fail();
        }
        catch ( ExecutionException e )
        {
            assertThat( e.getCause(), instanceOf( SSLException.class ) );
        }
    }

    @Test
    public void clientShouldNotCommunicateWithUntrustedServer() throws Exception
    {
        // given
        SslResource sslClientResource = selfSignedKeyId( 0 ).trustKeyId( UNRELATED_ID ).install( testDir.directory( "client" ) );
        SslResource sslServerResource = selfSignedKeyId( 1 ).trustKeyId( 0 ).install( testDir.directory( "server" ) );

        server = new SecureServer( makeSslContext( sslServerResource, true ) );

        server.start();
        client = new SecureClient( makeSslContext( sslClientResource, false ) );
        client.connect( server.port() );

        try
        {
            client.clientInitializer.handshakeFuture.get();
            fail();
        }
        catch ( ExecutionException e )
        {
            assertThat( e.getCause(), instanceOf( SSLException.class ) );
        }
    }

    @Test
    public void partiesWithMutualTrustThroughCAShouldNotCommunicateWhenServerRevoked() throws Exception
    {
        // given
        SslResource sslServerResource = caSignedKeyId( 0 ).trustSignedByCA().install( testDir.directory( "server" ) );
        SslResource sslClientResource = caSignedKeyId( 1 ).trustSignedByCA().revoke( 0 ).install( testDir.directory( "client" ) );

        server = new SecureServer( makeSslContext( sslServerResource, true ) );

        server.start();
        client = new SecureClient( makeSslContext( sslClientResource, false ) );
        client.connect( server.port() );

        try
        {
            client.clientInitializer.handshakeFuture.get();
            fail( "Server should have been revoked" );
        }
        catch ( ExecutionException e )
        {
            assertThat( e.getCause(), instanceOf( SSLException.class ) );
        }
    }

    @Test
    public void partiesWithMutualTrustThroughCAShouldNotCommunicateWhenClientRevoked() throws Exception
    {
        // given
        SslResource sslServerResource = caSignedKeyId( 0 ).trustSignedByCA().revoke( 1 ).install( testDir.directory( "server" ) );
        SslResource sslClientResource = caSignedKeyId( 1 ).trustSignedByCA().install( testDir.directory( "client" ) );

        server = new SecureServer( makeSslContext( sslServerResource, true ) );

        server.start();
        client = new SecureClient( makeSslContext( sslClientResource, false ) );
        client.connect( server.port() );

        try
        {
            client.clientInitializer.handshakeFuture.get();
            fail( "Client should have been revoked" );
        }
        catch ( ExecutionException e )
        {
            assertThat( e.getCause(), instanceOf( SSLException.class ) );
        }
    }

    @Test
    public void shouldSupportOpenSSLOnSupportedPlatforms() throws Exception
    {
        // depends on the statically linked uber-jar with boring ssl: http://netty.io/wiki/forked-tomcat-native.html
        assumeTrue( SystemUtils.IS_OS_WINDOWS || SystemUtils.IS_OS_LINUX || SystemUtils.IS_OS_MAC_OSX );
        assumeThat( System.getProperty( "os.arch" ), equalTo( "x86_64" ) );
        assumeThat( SystemUtils.JAVA_VENDOR, isOneOf( "Oracle Corporation", "Sun Microsystems Inc." ) );

        // given
        SslResource sslServerResource = selfSignedKeyId( 0 ).trustKeyId( 1 ).install( testDir.directory( "server" ) );
        SslResource sslClientResource = selfSignedKeyId( 1 ).trustKeyId( 0 ).install( testDir.directory( "client" ) );

        server = new SecureServer( makeSslContext( sslServerResource, true, SslProvider.OPENSSL.name() ) );

        server.start();
        client = new SecureClient( makeSslContext( sslClientResource, false, SslProvider.OPENSSL.name() ) );
        client.connect( server.port() );

        // when
        ByteBuf request = ByteBufAllocator.DEFAULT.buffer().writeBytes( REQUEST );
        client.channel.writeAndFlush( request );

        // then
        expected = ByteBufAllocator.DEFAULT.buffer().writeBytes( RESPONSE );
        client.clientInitializer.handshakeFuture.get();
        client.assertResponse( expected );
    }

    private SslContext makeSslContext( SslResource sslResource, boolean forServer ) throws CertificateException, IOException
    {
        return makeSslContext( sslResource, forServer, SslProvider.JDK.name() );
    }

    private SslContext makeSslContext( SslResource sslResource, boolean forServer, String sslProvider ) throws CertificateException, IOException
    {
        Map<String,String> config = new HashMap<>();
        config.put( SslSystemSettings.netty_ssl_provider.name(), sslProvider );

        SslPolicyConfig policyConfig = new SslPolicyConfig( "default" );
        File baseDirectory = sslResource.privateKey().getParentFile();
        new File( baseDirectory, "trusted" ).mkdirs();
        new File( baseDirectory, "revoked" ).mkdirs();

        config.put( policyConfig.base_directory.name(), baseDirectory.getPath() );
        config.put( policyConfig.private_key.name(), sslResource.privateKey().getPath() );
        config.put( policyConfig.public_certificate.name(), sslResource.publicCertificate().getPath() );
        config.put( policyConfig.trusted_dir.name(), sslResource.trustedDirectory().getPath() );
        config.put( policyConfig.revoked_dir.name(), sslResource.revokedDirectory().getPath() );

        SslPolicyLoader sslPolicyFactory = SslPolicyLoader.create( Config.fromSettings( config ).build(), NullLogProvider.getInstance() );

        SslPolicy sslPolicy = sslPolicyFactory.getPolicy( "default" );
        return forServer ? sslPolicy.nettyServerContext() : sslPolicy.nettyClientContext();
    }

    private class SecureServer
    {
        SslContext sslContext;
        Channel channel;
        NioEventLoopGroup eventLoopGroup;

        SecureServer( SslContext sslContext )
        {
            this.sslContext = sslContext;
        }

        private void start()
        {
            eventLoopGroup = new NioEventLoopGroup();
            ServerBootstrap bootstrap = new ServerBootstrap()
                    .group( eventLoopGroup )
                    .channel( NioServerSocketChannel.class )
                    .option( ChannelOption.SO_REUSEADDR, true )
                    .localAddress( 0 )
                    .childHandler( new ChannelInitializer<SocketChannel>()
                    {
                        @Override
                        protected void initChannel( SocketChannel ch ) throws Exception
                        {
                            ChannelPipeline pipeline = ch.pipeline();

                            SSLEngine sslEngine = sslContext.newEngine( ch.alloc() );
                            sslEngine.setNeedClientAuth( true );
                            SslHandler sslHandler = new SslHandler( sslEngine );
                            pipeline.addLast( sslHandler );

                            pipeline.addLast( new Responder() );
                        }
                    } );

            channel = bootstrap.bind().syncUninterruptibly().channel();
        }

        private void stop()
        {
            channel.close().awaitUninterruptibly();
            channel = null;
            eventLoopGroup.shutdownGracefully( 0, 0, SECONDS );
        }

        private int port()
        {
            return ((InetSocketAddress) channel.localAddress()).getPort();
        }
    }

    private class SecureClient
    {
        ClientInitializer clientInitializer;
        Bootstrap bootstrap;
        NioEventLoopGroup eventLoopGroup;
        Channel channel;
        Bucket bucket = new Bucket();

        SecureClient( SslContext sslContext )
        {
            eventLoopGroup = new NioEventLoopGroup();
            clientInitializer = new ClientInitializer( sslContext, bucket );
            bootstrap = new Bootstrap()
                    .group( eventLoopGroup )
                    .channel( NioSocketChannel.class )
                    .handler( clientInitializer );
        }

        @SuppressWarnings( "SameParameterValue" )
        void connect( int port )
        {
            ChannelFuture channelFuture = bootstrap.connect( "localhost", port ).awaitUninterruptibly();
            channel = channelFuture.channel();
        }

        void disconnect()
        {
            if ( channel != null )
            {
                channel.close().awaitUninterruptibly();
                eventLoopGroup.shutdownGracefully( 0, 0, SECONDS );
            }

            bucket.collectedData.release();
        }

        void assertResponse( ByteBuf expected ) throws InterruptedException
        {
            assertEventually( channel.toString(), () -> bucket.collectedData, equalTo( expected ), 5, SECONDS );
        }
    }

    public class ClientInitializer extends ChannelInitializer<SocketChannel>
    {
        SslContext sslContext;
        private final Bucket bucket;
        private io.netty.util.concurrent.Future<Channel> handshakeFuture;

        ClientInitializer( SslContext sslContext, Bucket bucket )
        {
            this.sslContext = sslContext;
            this.bucket = bucket;
        }

        @Override
        protected void initChannel( SocketChannel channel ) throws Exception
        {
            ChannelPipeline pipeline = channel.pipeline();

            SSLEngine sslEngine = sslContext.newEngine( channel.alloc() );
            sslEngine.setUseClientMode( true );

            SslHandler sslHandler = new SslHandler( sslEngine );
            handshakeFuture = sslHandler.handshakeFuture();

            pipeline.addLast( sslHandler );
            pipeline.addLast( bucket );
        }
    }

    class Bucket extends SimpleChannelInboundHandler<ByteBuf>
    {
        private final ByteBuf collectedData;

        Bucket()
        {
            collectedData = ByteBufAllocator.DEFAULT.buffer();
        }

        @Override
        protected void channelRead0( ChannelHandlerContext ctx, ByteBuf msg ) throws Exception
        {
            collectedData.writeBytes( msg );
        }

        @Override
        public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause ) throws Exception
        {
            //cause.printStackTrace();
        }
    }

    private class Responder extends SimpleChannelInboundHandler<ByteBuf>
    {
        @Override
        protected void channelRead0( ChannelHandlerContext ctx, ByteBuf msg ) throws Exception
        {
            ctx.channel().writeAndFlush( ctx.alloc().buffer().writeBytes( RESPONSE ) );
        }

        @Override
        public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause ) throws Exception
        {
            //cause.printStackTrace();
        }
    }
}
