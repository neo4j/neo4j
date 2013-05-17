/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.ChannelPipeline;
import org.jboss.netty.channel.ChannelPipelineFactory;
import org.jboss.netty.channel.ChannelStateEvent;
import org.jboss.netty.channel.Channels;
import org.jboss.netty.channel.ExceptionEvent;
import org.jboss.netty.channel.MessageEvent;
import org.jboss.netty.channel.SimpleChannelHandler;
import org.jboss.netty.channel.group.ChannelGroup;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;

import org.neo4j.com.RequestContext.Tx;
import org.neo4j.helpers.Exceptions;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Triplet;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.nioneo.xa.NeoStoreXaDataSource;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.logging.Logging;

import static org.neo4j.com.DechunkingChannelBuffer.assertSameProtocolVersion;

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
public abstract class Server<T, R> extends Protocol implements ChannelPipelineFactory, Lifecycle
{

    private InetSocketAddress socketAddress;

    public interface Configuration
    {
        long getOldChannelThreshold();

        int getMaxConcurrentTransactions();

        int getChunkSize();

        HostnamePort getServerAddress();
    }

    static final byte INTERNAL_PROTOCOL_VERSION = 2;
    public static final int DEFAULT_BACKUP_PORT = 6362;

    // It's ok if there are more transactions, since these worker threads doesn't
    // do any actual work themselves, but spawn off other worker threads doing the
    // actual work. So this is more like a core Netty I/O pool worker size.
    public final static int DEFAULT_MAX_NUMBER_OF_CONCURRENT_TRANSACTIONS = 200;

    private ServerBootstrap bootstrap;
    private T requestTarget;
    private ChannelGroup channelGroup;
    private final Map<Channel, Pair<RequestContext, AtomicLong /*time last heard of*/>> connectedSlaveChannels =
            new ConcurrentHashMap<Channel, Pair<RequestContext, AtomicLong>>();
    private ExecutorService executor;
    private ExecutorService workerExecutor;
    private ExecutorService targetCallExecutor;
    private StringLogger msgLog;
    private final Map<Channel, PartialRequest> partialRequests =
            new ConcurrentHashMap<Channel, PartialRequest>();
    private Configuration config;
    private final int frameLength;
    private volatile boolean shuttingDown;

    // Executor for channels that we know should be finished, but can't due to being
    // active at the moment.
    private ExecutorService unfinishedTransactionExecutor;

    // This is because there's a bug in Netty causing some channelClosed/channelDisconnected
    // events to not be sent. This is merely a safety net to catch the remained of the closed
    // channels that netty doesn't tell us about.
    private ScheduledExecutorService silentChannelExecutor;

    private final byte applicationProtocolVersion;
    private long oldChannelThresholdMillis;
    private TxChecksumVerifier txVerifier;
    private int chunkSize;

    public Server( T requestTarget, Configuration config, Logging logging, int frameLength,
                   byte applicationProtocolVersion, TxChecksumVerifier txVerifier )
    {
        this.requestTarget = requestTarget;
        this.config = config;
        this.frameLength = frameLength;
        this.applicationProtocolVersion = applicationProtocolVersion;
        this.msgLog = logging.getMessagesLog( getClass() );
        this.txVerifier = txVerifier;
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
        executor = Executors.newCachedThreadPool( new NamedThreadFactory( "Server receiving" ) );
        workerExecutor = Executors.newCachedThreadPool( new NamedThreadFactory( "Server receiving" ) );
        targetCallExecutor = Executors.newCachedThreadPool( new NamedThreadFactory( getClass().getSimpleName() + ":"
                + config.getServerAddress().getPort() ) );
        unfinishedTransactionExecutor = Executors.newScheduledThreadPool( 2, new NamedThreadFactory( "Unfinished " +
                "transactions" ) );
        silentChannelExecutor = Executors.newSingleThreadScheduledExecutor( new NamedThreadFactory( "Silent channel " +
                "reaper" ) );
        silentChannelExecutor.scheduleWithFixedDelay( silentChannelFinisher(), 5, 5, TimeUnit.SECONDS );
        bootstrap = new ServerBootstrap( new NioServerSocketChannelFactory(
                executor, workerExecutor, config.getMaxConcurrentTransactions() ) );
        bootstrap.setPipelineFactory( this );

        Channel channel = null;
        socketAddress = null;

        // Try binding to any port in the port range
        int[] ports = config.getServerAddress().getPorts();

        ChannelException ex = null;

        for ( int port = ports[0]; port <= ports[1]; port++ )
        {
            if ( config.getServerAddress().getHost() == null )
            {
                socketAddress = new InetSocketAddress( InetAddress.getLocalHost().getHostAddress(), port );
            }
            else
            {
                socketAddress = new InetSocketAddress( config.getServerAddress().getHost(), port );
            }
            try
            {
                channel = bootstrap.bind( socketAddress );
                ex = null;
                break;
            }
            catch ( ChannelException e )
            {
                ex = e;
            }
        }

        if ( ex != null )
        {
            msgLog.logMessage( "Failed to bind server to " + socketAddress, ex );
            executor.shutdown();
            workerExecutor.shutdown();
            throw new IOException( ex );
        }

        channelGroup = new DefaultChannelGroup();
        channelGroup.add( channel );
        msgLog.logMessage( getClass().getSimpleName() + " communication server started and bound to " + socketAddress );
    }

    @Override
    public void stop() throws Throwable
    {
        // Close all open connections
        shuttingDown = true;

        targetCallExecutor.shutdown();
        targetCallExecutor.awaitTermination( 10, TimeUnit.SECONDS );
        unfinishedTransactionExecutor.shutdown();
        unfinishedTransactionExecutor.awaitTermination( 10, TimeUnit.SECONDS );
        silentChannelExecutor.shutdown();
        silentChannelExecutor.awaitTermination( 10, TimeUnit.SECONDS );

        channelGroup.close().awaitUninterruptibly();

        bootstrap.releaseExternalResources();
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
                Map<Channel, Boolean/*starting to get old?*/> channels = new HashMap<Channel, Boolean>();
                synchronized ( connectedSlaveChannels )
                {
                    for ( Map.Entry<Channel, Pair<RequestContext, AtomicLong>> channel : connectedSlaveChannels
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
                for ( Map.Entry<Channel, Boolean> channel : channels.entrySet() )
                {
                    if ( channel.getValue() || !channel.getKey().isOpen() || !channel.getKey().isConnected() ||
                            !channel.getKey().isBound() )
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

    public ChannelPipeline getPipeline() throws Exception
    {
        ChannelPipeline pipeline = Channels.pipeline();
        addLengthFieldPipes( pipeline, frameLength );
        pipeline.addLast( "serverHandler", new ServerHandler() );
        return pipeline;
    }

    private class ServerHandler extends SimpleChannelHandler
    {
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
                msgLog.logMessage( "Error handling request", e );
                ctx.getChannel().close();
                tryToFinishOffChannel( ctx.getChannel() );
                throw Exceptions.launderedException( e );
            }
        }

        @Override
        public void channelClosed( ChannelHandlerContext ctx, ChannelStateEvent e )
                throws Exception
        {
            super.channelClosed( ctx, e );

            if ( !ctx.getChannel().isOpen() )
            {
                tryToFinishOffChannel( ctx.getChannel() );
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
                tryToFinishOffChannel( ctx.getChannel() );
            }
        }


        @Override
        public void exceptionCaught( ChannelHandlerContext ctx, ExceptionEvent e ) throws Exception
        {
            msgLog.warn( "Exception from Netty", e.getCause() );
        }
    }

    protected void tryToFinishOffChannel( Channel channel )
    {
        Pair<RequestContext, AtomicLong> slave;
        synchronized ( connectedSlaveChannels )
        {
            slave = connectedSlaveChannels.remove( channel );
        }
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
            if ( shouldLogFailureToFinishOffChannel( failure ) )
            {
                msgLog.logMessage( "Could not finish off dead channel", failure );
            }
        }
    }

    protected boolean shouldLogFailureToFinishOffChannel( Throwable failure )
    {
        return true;
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
                    unfinishedTransactionExecutor.submit( newTransactionFinisher( slave ) );
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

    protected void handleRequest( ChannelBuffer buffer, final Channel channel ) throws IOException
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
            final ChunkingChannelBuffer chunkingBuffer = new ChunkingChannelBuffer( bufferToWriteTo, channel, chunkSize,
                    getInternalProtocolVersion(), applicationProtocolVersion );
            submitSilent( targetCallExecutor, targetCaller( type, channel, context, chunkingBuffer,
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
            final ChunkingChannelBuffer failureResponse = new ChunkingChannelBuffer( ChannelBuffers.dynamicBuffer(),
                    channel,
                    chunkSize, getInternalProtocolVersion(), applicationProtocolVersion );
            submitSilent( targetCallExecutor, new Runnable()
            {
                @Override
                public void run()
                {
                    writeFailureResponse( e, failureResponse );
                }
            } );
            return null;
        }
        return (byte) (header[0] & 0x1);
    }

    protected Runnable targetCaller( final RequestType<T> type, final Channel channel, final RequestContext context,
                                     final ChunkingChannelBuffer targetBuffer, final ChannelBuffer bufferToReadFrom )
    {
        return new Runnable()
        {
            @SuppressWarnings("unchecked")
            public void run()
            {
                Response<R> response = null;
                try
                {
                    response = type.getTargetCaller().call( requestTarget, context, bufferToReadFrom, targetBuffer );
                    type.getObjectSerializer().write( response.response(), targetBuffer );
                    writeStoreId( response.getStoreId(), targetBuffer );
                    writeTransactionStreams( response.transactions(), targetBuffer );
                    targetBuffer.done();
                    responseWritten( type, channel, context );
                }
                catch ( Throwable e )
                {
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
                    unmapSlave( channel );
                }
            }
        };
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
    }

    private static void writeStoreId( StoreId storeId, ChannelBuffer targetBuffer )
    {
        targetBuffer.writeBytes( storeId.serialize() );
    }

    private static void writeTransactionStreams( TransactionStream txStream, ChannelBuffer buffer ) throws IOException
    {
        if ( !txStream.hasNext() )
        {
            buffer.writeByte( 0 );
            return;
        }

        String[] datasources = txStream.dataSourceNames();
        assert datasources.length <= 255 : "too many data sources";
        buffer.writeByte( datasources.length );
        Map<String, Integer> datasourceId = new HashMap<String, Integer>();
        for ( int i = 0; i < datasources.length; i++ )
        {
            String datasource = datasources[i];
            writeString( buffer, datasource );
            datasourceId.put( datasource, i + 1/*0 means "no more transactions"*/ );
        }
        for ( Triplet<String, Long, TxExtractor> tx : IteratorUtil.asIterable( txStream ) )
        {
            buffer.writeByte( datasourceId.get( tx.first() ) );
            buffer.writeLong( tx.second() );
            BlockLogBuffer blockBuffer = new BlockLogBuffer( buffer );
            tx.third().extract( blockBuffer );
            blockBuffer.done();
        }
        buffer.writeByte( 0/*no more transactions*/ );
    }

    protected RequestContext readContext( ChannelBuffer buffer )
    {
        long sessionId = buffer.readLong();
        int machineId = buffer.readInt();
        int eventIdentifier = buffer.readInt();
        int txsSize = buffer.readByte();
        Tx[] lastAppliedTransactions = new Tx[txsSize];
        Tx neoTx = null;
        for ( int i = 0; i < txsSize; i++ )
        {
            String ds = readString( buffer );
            Tx tx = RequestContext.lastAppliedTx( ds, buffer.readLong() );
            lastAppliedTransactions[i] = tx;

            // Only perform checksum checks on the neo data source.
            if ( ds.equals( NeoStoreXaDataSource.DEFAULT_DATA_SOURCE_NAME ) )
            {
                neoTx = tx;
            }
        }
        int masterId = buffer.readInt();
        long checksum = buffer.readLong();

        // Only perform checksum checks on the neo data source. If there's none in the request
        // then don't perform any such check.
        if ( neoTx != null )
        {
            txVerifier.assertMatch( neoTx.getTxId(), masterId, checksum );
        }
        return new RequestContext( sessionId, machineId, eventIdentifier, lastAppliedTransactions, masterId, checksum );
    }

    protected abstract RequestType<T> getRequestContext( byte id );

    protected ChannelBuffer mapSlave( Channel channel, RequestContext slave )
    {
        synchronized ( connectedSlaveChannels )
        {
            // Checking for machineId -1 excludes the "empty" slave contexts
            // which some communication points pass in as context.
            if ( slave != null && slave.machineId() != RequestContext.EMPTY.machineId() )
            {
                Pair<RequestContext, AtomicLong> previous = connectedSlaveChannels.get( channel );
                if ( previous != null )
                {
                    previous.other().set( System.currentTimeMillis() );
                }
                else
                {
                    connectedSlaveChannels.put( channel, Pair.of( slave, new AtomicLong( System.currentTimeMillis() )
                    ) );
                }
            }
        }
        return ChannelBuffers.dynamicBuffer();
    }

    protected void unmapSlave( Channel channel )
    {
        synchronized ( connectedSlaveChannels )
        {
            connectedSlaveChannels.remove( channel );
        }
    }

    protected T getRequestTarget()
    {
        return requestTarget;
    }

    protected abstract void finishOffChannel( Channel channel, RequestContext context );

    public Map<Channel, RequestContext> getConnectedSlaveChannels()
    {
        Map<Channel, RequestContext> result = new HashMap<Channel, RequestContext>();
        synchronized ( connectedSlaveChannels )
        {
            for ( Map.Entry<Channel, Pair<RequestContext, AtomicLong>> entry : connectedSlaveChannels.entrySet() )
            {
                result.put( entry.getKey(), entry.getValue().first() );
            }
        }
        return result;
    }

    // =====================================================================
    // Just some methods which aren't really used when running an HA cluster,
    // but exposed so that other tools can reach that information.
    // =====================================================================

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
