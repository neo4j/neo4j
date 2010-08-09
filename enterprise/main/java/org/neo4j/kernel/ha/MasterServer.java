package org.neo4j.kernel.ha;

import java.io.IOException;
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
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.ha.MasterClient.RequestType;
import org.neo4j.kernel.impl.ha.IdAllocation;
import org.neo4j.kernel.impl.ha.Master;
import org.neo4j.kernel.impl.ha.Response;
import org.neo4j.kernel.impl.ha.SlaveContext;

/**
 * Sits on the master side, receiving serialized requests from slaves (via
 * {@link MasterClient}). Delegates actual work to {@link MasterImpl}.
 */
public class MasterServer implements ChannelPipelineFactory
{
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
                bootstrap.bind( new InetSocketAddress( MasterClient.PORT ) );
            }
        } );
    }

    private class ServerHandler extends SimpleChannelHandler
    {
        @Override
        public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception
        {
            ChannelBuffer message = (ChannelBuffer) e.getMessage();
            handleMessage( ctx, message );
        }

        @Override
        public void exceptionCaught( ChannelHandlerContext ctx, ExceptionEvent e ) throws Exception
        {
            // TODO
        }
    }

    public ChannelPipeline getPipeline() throws Exception
    {
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast( "frameDecoder",
                new LengthFieldBasedFrameDecoder( 100000, 0, 4, 0, 4 ) );
        pipeline.addLast( "frameEncoder", new LengthFieldPrepender( 4 ) );
        pipeline.addLast( "serverHandler", new ServerHandler() );
        return pipeline;
    }

    public void handleMessage( final ChannelHandlerContext ctx, ChannelBuffer message )
        throws IOException
    {
        RequestType type = RequestType.values()[message.readByte()];
        switch ( type )
        {
        case ALLOCATE_IDS:
            performRequest( ctx, message, new MasterCaller<IdAllocation>()
            {
                public Response<IdAllocation> callMaster( ChannelBuffer buffer )
                {
                    SlaveContext context = readSlaveContext( ctx, buffer );
                    IdType idType = IdType.values()[buffer.readByte()];
                    return realMaster.allocateIds( context, idType );
                }
            }, new Serializer<IdAllocation>()
            {
                public void write( IdAllocation idAllocation, ChannelBuffer buffer )
                        throws IOException
                {
                    buffer.writeInt( idAllocation.getIds().length );
                    for ( long id : idAllocation.getIds() )
                    {
                        buffer.writeLong( id );
                    }
                    buffer.writeLong( idAllocation.getHighestIdInUse() );
                    buffer.writeLong( idAllocation.getDefragCount() );
                }
            } );
            break;
        }
    }

    private <T> void performRequest( ChannelHandlerContext ctx, ChannelBuffer message,
            MasterCaller<T> caller, Serializer<T> serializer ) throws IOException
    {
        Response<T> response = caller.callMaster( message );
        serializer.write( response.response(), message );
        writeTransactionStreams( response, message );
    }
    
    private <T> void writeTransactionStreams( Response<T> response, ChannelBuffer buffer )
    {
        // TODO
    }
    
    private static SlaveContext readSlaveContext( ChannelHandlerContext ctx,
            ChannelBuffer message )
    {
        // TODO Auto-generated method stub
        return null;
    }
    
    private interface Serializer<T>
    {
        void write( T responseObject, ChannelBuffer buffer ) throws IOException;
    }
    
    private interface MasterCaller<T>
    {
        Response<T> callMaster( ChannelBuffer buffer );
    }
}
