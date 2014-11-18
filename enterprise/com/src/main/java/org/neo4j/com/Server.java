/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.com;

import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.group.ChannelGroup;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.concurrent.GlobalEventExecutor;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.helpers.Clock;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.logging.Logging;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;

import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.Executors.newSingleThreadScheduledExecutor;
import static org.neo4j.com.DechunkingChannelBuffer.assertSameProtocolVersion;
import static org.neo4j.com.Protocol.addLengthFieldPipes;
import static org.neo4j.com.Protocol.assertChunkSizeIsWithinFrameSize;
import static org.neo4j.helpers.NamedThreadFactory.daemon;
import static org.neo4j.helpers.NamedThreadFactory.named;

/**
 * Receives requests from {@link Client clients}. Delegates actual work to an instance
 * of a specified communication interface, injected in the constructor.
 * <p/>
 * frameLength vs. chunkSize: frameLength is the maximum and hardcoded size in each
 * Netty buffer created by this server and handed off to a {@link Client}. If the
 * client has got a smaller frameLength than this server it will fail on reading a frame
 * that is bigger than what its frameLength.
 * chunkSize is the max size a buffer will have before it's sent off and a new buffer
 * allocated to continue writing to.
 * frameLength should be a constant for an implementation and must have the same value
 * on server as well as clients connecting to that server, whereas chunkSize very well
 * can be configurable and vary between server and client.
 *
 * @see Client
 */
@ChannelHandler.Sharable
public abstract class Server<T, R> extends ChannelInboundHandlerAdapter implements Lifecycle
{
    public interface Configuration
    {
        long getOldChannelThreshold();

        int getMaxConcurrentTransactions();

        int getChunkSize();

        HostnamePort getServerAddress();
    }

    // It's ok if there are more transactions, since these worker threads doesn't
    // do any actual work themselves, but spawn off other worker threads doing the
    // actual work. So this is more like a core Netty I/O pool worker size.
    public final static int DEFAULT_MAX_NUMBER_OF_CONCURRENT_TRANSACTIONS = 200;
    static final byte INTERNAL_PROTOCOL_VERSION = 2;
    private static final String INADDR_ANY = "0.0.0.0";
    private final T requestTarget;
    private final Map<Channel,Pair<RequestContext,AtomicLong /*time last heard of*/>> connectedSlaveChannels =
            new ConcurrentHashMap<>();
    private final StringLogger msgLog;
    private final Map<Channel,PartialRequest> partialRequests = new ConcurrentHashMap<>();
    private final Configuration config;
    private final int frameLength;
    private final ByteCounterMonitor byteCounterMonitor;
    private final RequestMonitor requestMonitor;
    private final Clock clock;
    private final byte applicationProtocolVersion;
    private final TxChecksumVerifier txVerifier;
    private ChannelGroup channelGroup;
    private ExecutorService targetCallExecutor;
    private volatile boolean shuttingDown;
    private InetSocketAddress socketAddress;
    // Executor for channels that we know should be finished, but can't due to being
    // active at the moment.
    private ExecutorService unfinishedTransactionExecutor;
    // This is because there's a bug in Netty causing some channelClosed/channelDisconnected
    // events to not be sent. This is merely a safety net to catch the remained of the closed
    // channels that netty doesn't tell us about.
    private ScheduledExecutorService silentChannelExecutor;
    private long oldChannelThresholdMillis;
    private int chunkSize;

    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;

    public Server( T requestTarget, Configuration config, Logging logging, int frameLength,
                   ProtocolVersion protocolVersion, TxChecksumVerifier txVerifier, Clock clock, ByteCounterMonitor
            byteCounterMonitor, RequestMonitor requestMonitor )
    {
        this.requestTarget = requestTarget;
        this.config = config;
        this.frameLength = frameLength;
        this.applicationProtocolVersion = protocolVersion.getApplicationProtocol();
        this.msgLog = logging.getMessagesLog( getClass() );
        this.txVerifier = txVerifier;
        this.clock = clock;
        this.byteCounterMonitor = byteCounterMonitor;
        this.requestMonitor = requestMonitor;
    }

    private static void writeStoreId( StoreId storeId, ByteBuf targetBuffer )
    {
        targetBuffer.writeLong( storeId.getCreationTime() );
        targetBuffer.writeLong( storeId.getRandomId() );
        targetBuffer.writeLong( storeId.getStoreVersion() );
        targetBuffer.writeLong( storeId.getUpgradeTime() );
        targetBuffer.writeLong( storeId.getUpgradeId() );
    }

    @Override
    public void init() throws Throwable
    {
    }

    @Override
    public void start() throws Throwable
    {
        this.oldChannelThresholdMillis = config.getOldChannelThreshold();
        chunkSize = config.getChunkSize();
        assertChunkSizeIsWithinFrameSize( chunkSize, frameLength );

        String className = getClass().getSimpleName();

        targetCallExecutor = newCachedThreadPool( named( className + ":" + config.getServerAddress().getPort() ) );
        unfinishedTransactionExecutor = newScheduledThreadPool( 2, named( "Unfinished transactions" ) );
        silentChannelExecutor = newSingleThreadScheduledExecutor( named( "Silent channel reaper" ) );
        silentChannelExecutor.scheduleWithFixedDelay( silentChannelFinisher(), 5, 5, TimeUnit.SECONDS );


        bossGroup = new NioEventLoopGroup(0, daemon( "Boss-" + className ));
        workerGroup = new NioEventLoopGroup(config.getMaxConcurrentTransactions(), daemon( "Worker-" + className ));

        ServerBootstrap bootstrap = new ServerBootstrap();
        bootstrap.group( bossGroup, workerGroup )
                .channel( NioServerSocketChannel.class )
                .option( ChannelOption.SO_BACKLOG, 100 )
                .option( ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT )
                .childOption( ChannelOption.TCP_NODELAY, true )
                .childHandler( new Initializer() );

        Channel channel = null;
        socketAddress = null;

        // Try binding to any port in the port range
        int[] ports = config.getServerAddress().getPorts();

        Throwable ex = null;

        for ( int port = ports[0]; port <= ports[1]; port++ )
        {
            if ( config.getServerAddress().getHost() == null ||
                 config.getServerAddress().getHost().equals( INADDR_ANY ) )
            {
                socketAddress = new InetSocketAddress( port );
            }
            else
            {
                socketAddress = new InetSocketAddress( config.getServerAddress().getHost(), port );
            }
            try
            {
                channel = bootstrap.bind( socketAddress ).sync().channel();
                ex = null;
                break;
            }
            catch ( Throwable e )
            {
                ex = e;
            }
        }

        if ( ex != null )
        {
            msgLog.logMessage( "Failed to bind server to " + socketAddress, ex );
            channelGroup.close().awaitUninterruptibly();
            targetCallExecutor.shutdownNow();
            unfinishedTransactionExecutor.shutdownNow();
            silentChannelExecutor.shutdownNow();
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
            throw new IOException( ex );
        }

        channelGroup = new DefaultChannelGroup( GlobalEventExecutor.INSTANCE );
        channelGroup.add( channel );
        msgLog.logMessage( className + " communication server started and bound to " + socketAddress );
    }

    @Override
    public void stop() throws Throwable
    {
        // Close all open connections
        shuttingDown = true;

        targetCallExecutor.shutdown();
        unfinishedTransactionExecutor.shutdown();
        silentChannelExecutor.shutdown();
        targetCallExecutor.awaitTermination( 10, TimeUnit.SECONDS );
        unfinishedTransactionExecutor.awaitTermination( 10, TimeUnit.SECONDS );
        silentChannelExecutor.awaitTermination( 10, TimeUnit.SECONDS );

        channelGroup.close().awaitUninterruptibly();

        bossGroup.shutdownGracefully();
        workerGroup.shutdownGracefully();
    }

    @Override
    public void shutdown() throws Throwable
    {
    }

    public InetSocketAddress getSocketAddress()
    {
        return socketAddress;
    }

    private Runnable silentChannelFinisher()
    {
        // This poller is here because sometimes Netty doesn't tell us when channels are
        // closed or disconnected. Most of the time it does, but this acts as a safety
        // net for those we don't get notifications for. When the bug is fixed remove this.
        return new Runnable()
        {
            @Override
            public void run()
            {
                Map<Channel,Boolean/*starting to get old?*/> channels = new HashMap<>();
                synchronized ( connectedSlaveChannels )
                {
                    for ( Map.Entry<Channel,Pair<RequestContext,AtomicLong>> channel : connectedSlaveChannels
                            .entrySet() )
                    {   // Has this channel been silent for a while?
                        long age = System.currentTimeMillis() - channel.getValue().other().get();
                        if ( age > oldChannelThresholdMillis )
                        {
                            msgLog.logMessage( "Found a silent channel " + channel + ", " + age );
                            channels.put( channel.getKey(), Boolean.TRUE );
                        }
                        else if ( age > oldChannelThresholdMillis / 2 )
                        {   // Then add it to a list to check
                            channels.put( channel.getKey(), Boolean.FALSE );
                        }
                    }
                }
                for ( Map.Entry<Channel,Boolean> channel : channels.entrySet() )
                {
                    if ( channel.getValue() || !channel.getKey().isActive() )
                    {
                        tryToFinishOffChannel( channel.getKey() );
                    }
                }
            }
        };
    }

    /**
     * Only exposed so that tests can control it. It's not configurable really.
     */
    protected byte getInternalProtocolVersion()
    {
        return INTERNAL_PROTOCOL_VERSION;
    }

    @Override
    public void channelActive( ChannelHandlerContext ctx ) throws Exception
    {
        channelGroup.add( ctx.channel() );
    }

    @Override
    public void channelRead( ChannelHandlerContext ctx, Object msg ) throws Exception
    {
        try
        {
            handleRequest( (ByteBuf) msg, ctx.channel() );
        }
        catch ( Throwable e )
        {
            msgLog.error( "Error handling request", e );

            // Attempt to reply to the client
            ChunkingChannelBuffer buffer = newChunkingBuffer( ctx.channel() );
            buffer.clear( /* failure = */true );
            writeFailureResponse( e, buffer );

            ctx.channel().close();
            tryToFinishOffChannel( ctx.channel() );
            throw Exceptions.launderedException( e );
        }
    }

    @Override
    public void channelInactive( ChannelHandlerContext ctx ) throws Exception
    {
        tryToFinishOffChannel( ctx.channel() );
        channelGroup.remove( ctx.channel() );
    }

    @Override
    public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause ) throws Exception
    {
        msgLog.warn( "Exception in network stack", cause );
    }

    protected void tryToFinishOffChannel( Channel channel )
    {
        Pair<RequestContext,AtomicLong> slave;
        slave = unmapSlave( channel );
        if ( slave == null )
        {
            return;
        }
        tryToFinishOffChannel( channel, slave.first() );
    }

    protected void tryToFinishOffChannel( Channel channel, RequestContext slave )
    {
        try
        {
            finishOffChannel( channel, slave );
            unmapSlave( channel );
        }
        catch ( Throwable failure ) // Unknown error trying to finish off the tx
        {
            submitSilent( unfinishedTransactionExecutor, newTransactionFinisher( slave ) );
            msgLog.logMessage( "Could not finish off dead channel", failure );
        }
    }

    private void submitSilent( ExecutorService service, Runnable job )
    {
        try
        {
            service.submit( job );
        }
        catch ( RejectedExecutionException e )
        {   // Don't scream and shout if we're shutting down, because a rejected execution
            // is expected at that time.
            if ( !shuttingDown )
            {
                throw e;
            }
        }
    }

    private Runnable newTransactionFinisher( final RequestContext slave )
    {
        return new Runnable()
        {
            @Override
            public void run()
            {
                try
                {
                    finishOffChannel( null, slave );
                }
                catch ( Throwable e )
                {
                    // Introduce some delay here. it becomes like a busy wait if it never succeeds
                    sleepNicely( 200 );
                    unfinishedTransactionExecutor.submit( this );
                }
            }

            private void sleepNicely( int millis )
            {
                try
                {
                    Thread.sleep( millis );
                }
                catch ( InterruptedException e )
                {
                    Thread.interrupted();
                }
            }
        };
    }

    protected void handleRequest( ByteBuf buffer, final Channel channel )
    {
        Byte continuation = readContinuationHeader( buffer, channel );
        if ( continuation == null )
        {
            return;
        }
        if ( continuation == ChunkingChannelBuffer.CONTINUATION_MORE )
        {
            PartialRequest partialRequest = partialRequests.get( channel );
            if ( partialRequest == null )
            {
                // This is the first chunk in a multi-chunk request
                RequestType<T> type = getRequestContext( buffer.readByte() );
                RequestContext context = readContext( buffer );
                ByteBuf targetBuffer = mapSlave( channel, context );
                partialRequest = new PartialRequest( type, context, targetBuffer );
                partialRequests.put( channel, partialRequest );
            }
            partialRequest.add( buffer );
        }
        else
        {
            PartialRequest partialRequest = partialRequests.remove( channel );
            RequestType<T> type;
            RequestContext context;
            ByteBuf targetBuffer;
            ByteBuf bufferToReadFrom;
            ByteBuf bufferToWriteTo;
            if ( partialRequest == null )
            {
                // This is the one and single chunk in the request
                type = getRequestContext( buffer.readByte() );
                context = readContext( buffer );
                targetBuffer = mapSlave( channel, context );
                bufferToReadFrom = buffer;
                bufferToWriteTo = targetBuffer;
            }
            else
            {
                // This is the last chunk in a multi-chunk request
                type = partialRequest.type;
                context = partialRequest.context;
                targetBuffer = partialRequest.buffer;
                partialRequest.add( buffer );
                bufferToReadFrom = targetBuffer;
                bufferToWriteTo = channel.alloc().buffer();
            }

            bufferToWriteTo.clear();
            final ChunkingChannelBuffer chunkingBuffer = new ChunkingChannelBuffer( bufferToWriteTo, channel, chunkSize,
                    getInternalProtocolVersion(), applicationProtocolVersion );
            submitSilent( targetCallExecutor, new TargetCaller( type, channel, context, chunkingBuffer,
                    bufferToReadFrom ) );
        }
    }

    private Byte readContinuationHeader( ByteBuf buffer, final Channel channel )
    {
        byte[] header = new byte[2];
        buffer.readBytes( header );
        try
        {   // Read request header and assert correct internal/application protocol version
            assertSameProtocolVersion( header, getInternalProtocolVersion(), applicationProtocolVersion );
        }
        catch ( final IllegalProtocolVersionException e )
        {   // Version mismatch, fail with a good exception back to the client
            submitSilent( targetCallExecutor, new Runnable()
            {
                @Override
                public void run()
                {
                    writeFailureResponse( e, newChunkingBuffer( channel ) );
                }
            } );
            return null;
        }
        return (byte) (header[0] & 0x1);
    }

    protected void writeFailureResponse( Throwable exception, ChunkingChannelBuffer buffer )
    {
        try
        {
            ByteArrayOutputStream bytes = new ByteArrayOutputStream();
            ObjectOutputStream out = new ObjectOutputStream( bytes );
            out.writeObject( exception );
            out.close();
            buffer.writeBytes( bytes.toByteArray() );
            buffer.done();
        }
        catch ( IOException e )
        {
            msgLog.logMessage( "Couldn't send cause of error to client", exception );
        }
    }

    protected void responseWritten( RequestType<T> type, Channel channel, RequestContext context )
    {
        /*
         * This is here to ensure that channels that have stuff written to them for a long time,
         * long transaction
         * pulls and store copies (mainly the latter), will not timeout and have their
         * transactions rolled back.
         * This is actually not a problem, since both mentioned above have no transaction
         * associated with them
         * but it is more sanitary and leaves less exceptions in the logs
         * Each time a write completes, simply update the corresponding channel's timestamp.
         */
        Pair<RequestContext, AtomicLong> slave = connectedSlaveChannels.get( channel );
        if ( slave != null )
        {
            slave.other().set( clock.currentTimeMillis() );
        }
    }

    protected RequestContext readContext( ByteBuf buffer )
    {
        long sessionId = buffer.readLong();
        int machineId = buffer.readInt();
        int eventIdentifier = buffer.readInt();
        long neoTx = buffer.readLong();
        int masterId = buffer.readInt();
        long checksum = buffer.readLong();

        RequestContext readRequestContext = new RequestContext( sessionId, machineId, eventIdentifier, neoTx, masterId,
                checksum );
        // Only perform checksum checks on the neo data source. If there's none in the request
        // then don't perform any such check.
        if ( neoTx > 0 )
        {
            txVerifier.assertMatch( neoTx, masterId, checksum );
        }
        return readRequestContext;
    }

    protected abstract RequestType<T> getRequestContext( byte id );

    protected ByteBuf mapSlave( Channel channel, RequestContext slave )
    {
        synchronized ( connectedSlaveChannels )
        {
            // Checking for machineId -1 excludes the "empty" slave contexts
            // which some communication points pass in as context.
            if ( slave != null && slave.machineId() != RequestContext.EMPTY.machineId() )
            {
                Pair<RequestContext,AtomicLong> previous = connectedSlaveChannels.get( channel );
                if ( previous != null )
                {
                    previous.other().set( System.currentTimeMillis() );
                }
                else
                {
                    connectedSlaveChannels.put( channel,
                            Pair.of( slave, new AtomicLong( System.currentTimeMillis() ) ) );
                }
            }
        }
        return channel.alloc().buffer();
    }

    protected Pair<RequestContext,AtomicLong> unmapSlave( Channel channel )
    {
        synchronized ( connectedSlaveChannels )
        {
            return connectedSlaveChannels.remove( channel );
        }
    }

    protected T getRequestTarget()
    {
        return requestTarget;
    }

    protected abstract void finishOffChannel( Channel channel, RequestContext context );

    public Map<Channel,RequestContext> getConnectedSlaveChannels()
    {
        Map<Channel,RequestContext> result = new HashMap<>();
        synchronized ( connectedSlaveChannels )
        {
            for ( Map.Entry<Channel,Pair<RequestContext,AtomicLong>> entry : connectedSlaveChannels.entrySet() )
            {
                result.put( entry.getKey(), entry.getValue().first() );
            }
        }
        return result;
    }

    private ChunkingChannelBuffer newChunkingBuffer( Channel channel )
    {
        ByteBuf buffer = channel.alloc().buffer();
        try
        {
            return new ChunkingChannelBuffer( buffer, channel,
                    chunkSize, getInternalProtocolVersion(), applicationProtocolVersion );
        }
        finally
        {
            // ChunkingBuffer will .retain() in its constructor, a temporary hack before we set this
            // up to use netty buffer pooling.
            buffer.release();
        }
    }

    private class TargetCaller implements Response.Handler, Runnable
    {
        private final RequestType<T> type;
        private final Channel channel;
        private final RequestContext context;
        private final ChunkingChannelBuffer targetBuffer;
        private final ByteBuf bufferToReadFrom;

        TargetCaller( RequestType<T> type, Channel channel, RequestContext context,
                      ChunkingChannelBuffer targetBuffer, ByteBuf bufferToReadFrom )
        {
            this.type = type;
            this.channel = channel;
            this.context = context;
            this.targetBuffer = targetBuffer;
            this.bufferToReadFrom = bufferToReadFrom;
        }

        @Override
        @SuppressWarnings( "unchecked" )
        public void run()
        {
            Map<String,String> requestContext = new HashMap<>();
            requestContext.put( "type", type.toString() );
            requestContext.put( "remoteClient", channel.remoteAddress().toString() );
            requestContext.put( "slaveContext", context.toString() );
            requestMonitor.beginRequest( requestContext );
            Response<R> response = null;
            Throwable failure = null;
            try
            {
                unmapSlave( channel );
                response = type.getTargetCaller().call( requestTarget, context, bufferToReadFrom, targetBuffer );
                type.getObjectSerializer().write( response.response(), targetBuffer );
                writeStoreId( response.getStoreId(), targetBuffer );
                response.accept( this );
                targetBuffer.done();
                responseWritten( type, channel, context );
            }
            catch ( Throwable e )
            {
                failure = e;
                targetBuffer.clear( true );
                writeFailureResponse( e, targetBuffer );
                tryToFinishOffChannel( channel, context );
                throw Exceptions.launderedException( e );
            }
            finally
            {
                if ( response != null )
                {
                    response.close();
                }
                requestMonitor.endRequest( failure );
            }
        }

        @Override
        public void obligation( long txId ) throws IOException
        {
            targetBuffer.writeByte( -1 );
            targetBuffer.writeLong( txId );
        }

        @Override
        public Visitor<CommittedTransactionRepresentation,IOException> transactions()
        {
            targetBuffer.writeByte( 1 );
            return new CommittedTransactionSerializer( targetBuffer );
        }
    }

    private class PartialRequest
    {
        final RequestContext context;
        final ByteBuf buffer;
        final RequestType<T> type;

        public PartialRequest( RequestType<T> type, RequestContext context, ByteBuf buffer )
        {
            this.type = type;
            this.context = context;
            this.buffer = buffer;
        }

        public void add( ByteBuf buffer )
        {
            this.buffer.writeBytes( buffer );
        }
    }

    private class Initializer extends ChannelInitializer<SocketChannel>
    {
        @Override
        protected void initChannel( SocketChannel ch ) throws Exception
        {
            try
            {
                ChannelPipeline pipeline = ch.pipeline();
                pipeline.addLast( "monitor", new MonitorChannelHandler( byteCounterMonitor ) );
                addLengthFieldPipes( pipeline, frameLength );
                pipeline.addLast( "serverHandler", Server.this );
            }
            catch(Throwable e)
            {
                e.printStackTrace();
            }
        }
    }
}
