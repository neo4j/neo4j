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
package org.neo4j.com;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.WriteCompletionEvent;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.neo4j.com.monitor.RequestMonitor;
import org.neo4j.helpers.Clock;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.collection.Visitor;
import org.neo4j.kernel.impl.store.StoreId;
import org.neo4j.kernel.impl.transaction.CommittedTransactionRepresentation;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

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
 * <p>
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
public abstract class Server<T, R> extends SimpleChannelHandler implements ChannelPipelineFactory, Lifecycle,
        ChannelCloser
{

    private final LogProvider logProvider;
    private ScheduledExecutorService silentChannelExecutor;

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
    private final T requestTarget;
    private IdleChannelReaper connectedSlaveChannels;
    private final Log msgLog;
    private final Map<Channel,PartialRequest> partialRequests = new ConcurrentHashMap<>();
    private final Configuration config;
    private final int frameLength;
    private final ByteCounterMonitor byteCounterMonitor;
    private final RequestMonitor requestMonitor;
    private final byte applicationProtocolVersion;
    private final TxChecksumVerifier txVerifier;
    private ServerBootstrap bootstrap;
    private ChannelGroup channelGroup;
    private ExecutorService targetCallExecutor;
    private volatile boolean shuttingDown;
    private InetSocketAddress socketAddress;
    // Executor for channels that we know should be finished, but can't due to being
    // active at the moment.
    private ExecutorService unfinishedTransactionExecutor;
    private int chunkSize;

    public Server( T requestTarget, Configuration config, LogProvider logProvider, int frameLength,
                   ProtocolVersion protocolVersion, TxChecksumVerifier txVerifier, Clock clock, ByteCounterMonitor
            byteCounterMonitor, RequestMonitor requestMonitor )
    {
        this.requestTarget = requestTarget;
        this.config = config;
        this.frameLength = frameLength;
        this.applicationProtocolVersion = protocolVersion.getApplicationProtocol();
        this.logProvider = logProvider;
        this.msgLog = this.logProvider.getLog( getClass() );
        this.txVerifier = txVerifier;
        this.byteCounterMonitor = byteCounterMonitor;
        this.requestMonitor = requestMonitor;
        this.connectedSlaveChannels = new IdleChannelReaper( this, logProvider, clock, config.getOldChannelThreshold() );
        this.chunkSize = config.getChunkSize();
        assertChunkSizeIsWithinFrameSize( chunkSize, frameLength );
    }

    private static void writeStoreId( StoreId storeId, ChannelBuffer targetBuffer )
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
        chunkSize = config.getChunkSize();
        assertChunkSizeIsWithinFrameSize( chunkSize, frameLength );

        String className = getClass().getSimpleName();

        targetCallExecutor = newCachedThreadPool( named( className + ":" + config.getServerAddress().getPort() ) );
        unfinishedTransactionExecutor = newScheduledThreadPool( 2, named( "Unfinished transactions" ) );
        silentChannelExecutor = newSingleThreadScheduledExecutor( named( "Silent channel reaper" ) );
        silentChannelExecutor.scheduleWithFixedDelay( connectedSlaveChannels, 5, 5, TimeUnit.SECONDS );
    }

    @Override
    public void start() throws Throwable
    {
        String className = getClass().getSimpleName();
        ExecutorService bossExecutor = newCachedThreadPool( daemon( "Boss-" + className ) );
        ExecutorService workerExecutor = newCachedThreadPool( daemon( "Worker-" + className ) );
        bootstrap = new ServerBootstrap( new NioServerSocketChannelFactory(
                bossExecutor, workerExecutor, config.getMaxConcurrentTransactions() ) );
        bootstrap.setPipelineFactory( this );

        PortRangeSocketBinder portRangeSocketBinder = new PortRangeSocketBinder( bootstrap );
        try
        {
            Connection connection = portRangeSocketBinder.bindToFirstAvailablePortInRange( config.getServerAddress() );
            Channel channel = connection.getChannel();
            socketAddress = connection.getSocketAddress();

            channelGroup = new DefaultChannelGroup();
            channelGroup.add( channel );
            msgLog.info( className + " communication server started and bound to " + socketAddress );
        }
        catch ( Exception ex )
        {
            msgLog.error( "Failed to bind server to " + socketAddress, ex );
            bootstrap.releaseExternalResources();
            targetCallExecutor.shutdownNow();
            unfinishedTransactionExecutor.shutdownNow();
            silentChannelExecutor.shutdownNow();
            throw new IOException( ex );
        }
    }

    @Override
    public void stop() throws Throwable
    {
        String name = getClass().getSimpleName();
        msgLog.info( name + " communication server shutting down and unbinding from  " + socketAddress );

        shuttingDown = true;
        channelGroup.close().awaitUninterruptibly();
        bootstrap.releaseExternalResources();
    }

    @Override
    public void shutdown() throws Throwable
    {
        targetCallExecutor.shutdown();
        targetCallExecutor.awaitTermination( 10, TimeUnit.SECONDS );
        unfinishedTransactionExecutor.shutdown();
        unfinishedTransactionExecutor.awaitTermination( 10, TimeUnit.SECONDS );
        silentChannelExecutor.shutdown();
        silentChannelExecutor.awaitTermination( 10, TimeUnit.SECONDS );
    }

    public InetSocketAddress getSocketAddress()
    {
        return socketAddress;
    }

    /**
     * Only exposed so that tests can control it. It's not configurable really.
     */
    protected byte getInternalProtocolVersion()
    {
        return INTERNAL_PROTOCOL_VERSION;
    }

    @Override
    public ChannelPipeline getPipeline() throws Exception
    {
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast( "monitor", new MonitorChannelHandler( byteCounterMonitor ) );
        addLengthFieldPipes( pipeline, frameLength );
        pipeline.addLast( "serverHandler", this );
        return pipeline;
    }

    @Override
    public void channelOpen( ChannelHandlerContext ctx, ChannelStateEvent e ) throws Exception
    {
        channelGroup.add( e.getChannel() );
    }

    @Override
    public void messageReceived( ChannelHandlerContext ctx, MessageEvent event )
            throws Exception
    {
        try
        {
            ChannelBuffer message = (ChannelBuffer) event.getMessage();
            handleRequest( message, event.getChannel() );
        }
        catch ( Throwable e )
        {
            msgLog.error( "Error handling request", e );

            // Attempt to reply to the client
            ChunkingChannelBuffer buffer = newChunkingBuffer( event.getChannel() );
            buffer.clear( /* failure = */true );
            writeFailureResponse( e, buffer );

            ctx.getChannel().close();
            tryToCloseChannel( ctx.getChannel() );
            throw Exceptions.launderedException( e );
        }
    }

    @Override
    public void writeComplete( ChannelHandlerContext ctx, WriteCompletionEvent e ) throws Exception
    {
        /*
         * This is here to ensure that channels that have stuff written to them for a long time, long transaction
         * pulls and store copies (mainly the latter), will not timeout and have their transactions rolled back.
         * This is actually not a problem, since both mentioned above have no transaction associated with them
         * but it is more sanitary and leaves less exceptions in the logs
         * Each time a write completes, simply update the corresponding channel's timestamp.
         */
        if ( connectedSlaveChannels.update( ctx.getChannel() ) )
        {
            super.writeComplete( ctx, e );
        }
    }

    @Override
    public void channelClosed( ChannelHandlerContext ctx, ChannelStateEvent e )
            throws Exception
    {
        super.channelClosed( ctx, e );

        if ( !ctx.getChannel().isOpen() )
        {
            tryToCloseChannel( ctx.getChannel() );
        }

        channelGroup.remove( e.getChannel() );
    }

    @Override
    public void channelDisconnected( ChannelHandlerContext ctx, ChannelStateEvent e )
            throws Exception
    {
        super.channelDisconnected( ctx, e );

        if ( !ctx.getChannel().isConnected() )
        {
            tryToCloseChannel( ctx.getChannel() );
        }
    }

    @Override
    public void exceptionCaught( ChannelHandlerContext ctx, ExceptionEvent e ) throws Exception
    {
        msgLog.warn( "Exception from Netty", e.getCause() );
    }

    @Override
    public void tryToCloseChannel( Channel channel )
    {
        IdleChannelReaper.Request request = unmapSlave( channel );
        if ( request == null )
        {
            return;
        }
        tryToFinishOffChannel( channel, request.getRequestContext());
    }

    protected void tryToFinishOffChannel( Channel channel, RequestContext slave )
    {
        try
        {
            stopConversation( slave );
            unmapSlave( channel );
        }
        catch ( Throwable failure ) // Unknown error trying to finish off the tx
        {
            submitSilent( unfinishedTransactionExecutor, newTransactionFinisher( slave ) );
            msgLog.warn( "Could not finish off dead channel", failure );
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
                    stopConversation( slave );
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

    protected void handleRequest( ChannelBuffer buffer, final Channel channel )
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
                ChannelBuffer targetBuffer = mapSlave( channel, context );
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
            ChannelBuffer targetBuffer;
            ChannelBuffer bufferToReadFrom;
            ChannelBuffer bufferToWriteTo;
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
                bufferToWriteTo = ChannelBuffers.dynamicBuffer();
            }

            bufferToWriteTo.clear();
            ChunkingChannelBuffer chunkingBuffer = newChunkingBuffer( bufferToWriteTo, channel, chunkSize,
                    getInternalProtocolVersion(), applicationProtocolVersion );
            submitSilent( targetCallExecutor, new TargetCaller( type, channel, context, chunkingBuffer,
                    bufferToReadFrom ) );
        }
    }

    private Byte readContinuationHeader( ChannelBuffer buffer, final Channel channel )
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
            msgLog.warn( "Couldn't send cause of error to client", exception );
        }
    }

    protected void responseWritten( RequestType<T> type, Channel channel, RequestContext context )
    {
    }

    protected RequestContext readContext( ChannelBuffer buffer )
    {
        long sessionId = buffer.readLong();
        int machineId = buffer.readInt();
        int eventIdentifier = buffer.readInt();
        long neoTx = buffer.readLong();
        long checksum = buffer.readLong();

        RequestContext readRequestContext =
                new RequestContext( sessionId, machineId, eventIdentifier, neoTx, checksum );

        // verify checksum only if there are transactions committed in the store
        if ( neoTx > TransactionIdStore.BASE_TX_ID )
        {
            txVerifier.assertMatch( neoTx, checksum );
        }
        return readRequestContext;
    }

    protected abstract RequestType<T> getRequestContext( byte id );

    protected ChannelBuffer mapSlave( Channel channel, RequestContext slave )
    {
        // Checking for machineId -1 excludes the "empty" slave contexts
        // which some communication points pass in as context.
        if ( slave != null && slave.machineId() != RequestContext.EMPTY.machineId() )
        {
            connectedSlaveChannels.add( channel, slave );
        }
        return ChannelBuffers.dynamicBuffer();
    }

    protected IdleChannelReaper.Request unmapSlave( Channel channel )
    {
        return connectedSlaveChannels.remove( channel );
    }

    protected T getRequestTarget()
    {
        return requestTarget;
    }

    protected abstract void stopConversation( RequestContext context );

    private ChunkingChannelBuffer newChunkingBuffer( Channel channel )
    {
        return newChunkingBuffer( ChannelBuffers.dynamicBuffer(), channel, chunkSize, getInternalProtocolVersion(),
                applicationProtocolVersion );
    }

    protected ChunkingChannelBuffer newChunkingBuffer( ChannelBuffer bufferToWriteTo, Channel channel, int capacity,
            byte internalProtocolVersion, byte applicationProtocolVersion )
    {
        return new ChunkingChannelBuffer( bufferToWriteTo, channel, capacity, internalProtocolVersion,
                applicationProtocolVersion );
    }

    private class TargetCaller implements Response.Handler, Runnable
    {
        private final RequestType<T> type;
        private final Channel channel;
        private final RequestContext context;
        private final ChunkingChannelBuffer targetBuffer;
        private final ChannelBuffer bufferToReadFrom;

        TargetCaller( RequestType<T> type, Channel channel, RequestContext context,
                      ChunkingChannelBuffer targetBuffer, ChannelBuffer bufferToReadFrom )
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
            requestMonitor.beginRequest( channel.getRemoteAddress(), type, context );
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
        final ChannelBuffer buffer;
        final RequestType<T> type;

        public PartialRequest( RequestType<T> type, RequestContext context, ChannelBuffer buffer )
        {
            this.type = type;
            this.context = context;
            this.buffer = buffer;
        }

        public void add( ChannelBuffer buffer )
        {
            this.buffer.writeBytes( buffer );
        }
    }
}
