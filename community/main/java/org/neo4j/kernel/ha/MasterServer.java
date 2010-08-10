package org.neo4j.kernel.ha;

import java.net.InetSocketAddress;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
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
    public static final int PORT = 8901;
    
    private final Master realMaster;

    public MasterServer( Master realMaster )
    {
        this.realMaster = realMaster;
        ExecutorService executor = Executors.newCachedThreadPool();
        final ServerBootstrap bootstrap = new ServerBootstrap( new NioServerSocketChannelFactory(
                executor, executor ) );
        bootstrap.setPipelineFactory( this );
        executor.execute( new Runnable()
        {
            public void run()
            {
                bootstrap.bind( new InetSocketAddress( CommunicationProtocol.PORT ) );
            }
        } );
    }

    public ChannelPipeline getPipeline() throws Exception
    {
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast( "frameDecoder", new LengthFieldBasedFrameDecoder( 100000, 0, 4, 0, 4 ) );
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
            handleRequest( realMaster, ctx, message );
        }

        @Override
        public void exceptionCaught( ChannelHandlerContext ctx, ExceptionEvent e ) throws Exception
        {
            // TODO
        }
    }
}
