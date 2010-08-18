package org.neo4j.kernel.ha;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelFactory;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.handler.codec.frame.LengthFieldBasedFrameDecoder;
import org.jboss.netty.handler.codec.frame.LengthFieldPrepender;
import org.neo4j.kernel.impl.ha.Master;

/**
 * Sits on the master side, receiving serialized requests from slaves (via
 * {@link MasterClient}). Delegates actual work to {@link MasterImpl}.
 */
public class MasterServer extends CommunicationProtocol implements ChannelPipelineFactory
{
    private final ChannelFactory channelFactory;
    private final ServerBootstrap bootstrap;
    private final Master realMaster;
    private final ChannelGroup channelGroup;

    public MasterServer( Master realMaster, final int port )
    {
        this.realMaster = realMaster;
        ExecutorService executor = Executors.newCachedThreadPool();
        channelFactory = new NioServerSocketChannelFactory(
                executor, executor );
        bootstrap = new ServerBootstrap( channelFactory );
        bootstrap.setPipelineFactory( this );
        channelGroup = new DefaultChannelGroup();
        executor.execute( new Runnable()
        {
            public void run()
            {
                Channel channel = bootstrap.bind( new InetSocketAddress( port ) );
                // Add the "server" channel
                channelGroup.add( channel );
                System.out.println( "Master server bound to " + port );
            }
        } );
    }

    public ChannelPipeline getPipeline() throws Exception
    {
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast( "frameDecoder", new LengthFieldBasedFrameDecoder( 1000000, 0, 4, 0, 4 ) );
        pipeline.addLast( "frameEncoder", new LengthFieldPrepender( 4 ) );
        pipeline.addLast( "serverHandler", new ServerHandler() );
        return pipeline;
    }

    private class ServerHandler extends SimpleChannelHandler
    {
        @Override
        public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception
        {
            ChannelBuffer message = (ChannelBuffer) e.getMessage();
            // Add each "client" channel
            channelGroup.add( e.getChannel() );
            e.getChannel().write( handleRequest( realMaster, message ) );
        }

        @Override
        public void exceptionCaught( ChannelHandlerContext ctx, ExceptionEvent e ) throws Exception
        {
            // TODO
        }
    }
    
    public void shutdown()
    {
        // Close all open connections
        channelGroup.close().awaitUninterruptibly();
        channelFactory.releaseExternalResources();
    }
}
