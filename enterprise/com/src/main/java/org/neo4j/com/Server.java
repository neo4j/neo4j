/**
 * Copyright (c) 2002-2011 "Neo Technology,"
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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.jboss.netty.bootstrap.ServerBootstrap;
import org.jboss.netty.buffer.ChannelBuffer;
import org.jboss.netty.buffer.ChannelBuffers;
import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelException;
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
import org.neo4j.helpers.Pair;
import org.neo4j.helpers.Triplet;
import org.neo4j.helpers.collection.IteratorUtil;
import org.neo4j.kernel.impl.nioneo.store.StoreId;
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * Sits on the master side, receiving serialized requests from slaves (via
 * {@link Client}). Delegates actual work to {@link MasterImpl}.
 */
public abstract class Server<M, R> extends Protocol implements ChannelPipelineFactory
{
    public static final int DEFAULT_BACKUP_PORT = 6362;
    
    private final static int DEAD_CONNECTIONS_CHECK_INTERVAL = 3;
    protected final static int DEFAULT_MAX_NUMBER_OF_CONCURRENT_TRANSACTIONS = 200;

    private final ChannelFactory channelFactory;
    private final ServerBootstrap bootstrap;
    private final M realMaster;
    private final ChannelGroup channelGroup;
    private final ScheduledExecutorService deadConnectionsPoller;
    private final Map<Channel, SlaveContext> connectedSlaveChannels = new HashMap<Channel, SlaveContext>();
    private final Map<Channel, Pair<ChannelBuffer, ByteBuffer>> channelBuffers =
            new HashMap<Channel, Pair<ChannelBuffer,ByteBuffer>>();
    private final ExecutorService executor;
    private final StringLogger msgLog;
    private final Map<Channel, PartialRequest> partialRequests =
            Collections.synchronizedMap( new HashMap<Channel, PartialRequest>() );

    public Server( M realMaster, final int port, String storeDir )
    {
        this( realMaster, port, storeDir, DEFAULT_MAX_NUMBER_OF_CONCURRENT_TRANSACTIONS );
    }
    
    public Server( M realMaster, final int port, String storeDir, int maxNumberOfConcurrentTransactions )
    {
        this.realMaster = realMaster;
        this.msgLog = StringLogger.getLogger( storeDir + "/messages.log" );
        executor = Executors.newCachedThreadPool();
        channelFactory = new NioServerSocketChannelFactory(
                executor, executor, maxNumberOfConcurrentTransactions );
        bootstrap = new ServerBootstrap( channelFactory );
        bootstrap.setPipelineFactory( this );
        
        Channel channel;
        try
        {
            channel = bootstrap.bind( new InetSocketAddress( port ) );
        }
        catch ( ChannelException e )
        {
            msgLog.logMessage( "Failed to bind master server to port " + port, e );
            executor.shutdown();
            throw e;
        }
        channelGroup = new DefaultChannelGroup();
        // Add the "server" channel
        channelGroup.add( channel );
        msgLog.logMessage( getClass().getSimpleName() + " communication server started and bound to " + port, true );

        deadConnectionsPoller = new ScheduledThreadPoolExecutor( 1 );
        deadConnectionsPoller.scheduleWithFixedDelay( new Runnable()
        {
            public void run()
            {
                checkForDeadChannels();
            }
        }, DEAD_CONNECTIONS_CHECK_INTERVAL, DEAD_CONNECTIONS_CHECK_INTERVAL, TimeUnit.SECONDS );
    }

    public ChannelPipeline getPipeline() throws Exception
    {
        ChannelPipeline pipeline = Channels.pipeline();
        addLengthFieldPipes( pipeline );
        pipeline.addLast( "serverHandler", new ServerHandler() );
        return pipeline;
    }

    private class ServerHandler extends SimpleChannelHandler
    {
        @Override
        public void messageReceived( ChannelHandlerContext ctx, MessageEvent event )
                throws Exception
        {
            try
            {
                ChannelBuffer message = (ChannelBuffer) event.getMessage();
                handleRequest( message, event.getChannel() );
            }
            catch ( Exception e )
            {
                e.printStackTrace();
                throw e;
            }
        }

        @Override
        public void exceptionCaught( ChannelHandlerContext ctx, ExceptionEvent e ) throws Exception
        {
            e.getCause().printStackTrace();
        }
    }

    @SuppressWarnings( "unchecked" )
    protected void handleRequest( ChannelBuffer buffer, final Channel channel ) throws IOException
    {
        // TODO Too long method, refactor please
        byte continuation = buffer.readByte();
        if ( continuation == ChunkingChannelBuffer.CONTINUATION_MORE )
        {
            PartialRequest partialRequest = partialRequests.get( channel );
            if ( partialRequest == null )
            {
                // This is the first chunk
                RequestType<M> type = getRequestContext( buffer.readByte() );
                SlaveContext context = readContext( buffer );
                Pair<ChannelBuffer, ByteBuffer> targetBuffers = mapSlave( channel, context );
                partialRequest = new PartialRequest( type, context, targetBuffers );
                partialRequests.put( channel, partialRequest );
            }
            partialRequest.add( buffer );
        }
        else
        {
            PartialRequest partialRequest = partialRequests.remove( channel );
            RequestType<M> type = null;
            SlaveContext context = null;
            Pair<ChannelBuffer, ByteBuffer> targetBuffers;
            ChannelBuffer bufferToReadFrom = null;
            ChannelBuffer bufferToWriteTo = null;
            if ( partialRequest == null )
            {
                type = getRequestContext( buffer.readByte() );
                context = readContext( buffer );
                targetBuffers = mapSlave( channel, context );
                bufferToReadFrom = buffer;
                bufferToWriteTo = targetBuffers.first();
            }
            else
            {
                type = partialRequest.type;
                context = partialRequest.context;
                targetBuffers = partialRequest.buffers;
                partialRequest.add( buffer );
                bufferToReadFrom = targetBuffers.first();
                bufferToWriteTo = ChannelBuffers.dynamicBuffer();
            }

            bufferToWriteTo.clear();
            final ChunkingChannelBuffer chunkingBuffer = new ChunkingChannelBuffer( bufferToWriteTo, channel, MAX_FRAME_LENGTH );
            Response<R> response = type.getMasterCaller().callMaster( realMaster, context, bufferToReadFrom, chunkingBuffer );
            executor.submit( responseWriter( type, channel, context, chunkingBuffer, targetBuffers.other(), response ) );
        }
    }

    private Runnable responseWriter( final RequestType<M> type, final Channel channel, final SlaveContext context,
            final ChunkingChannelBuffer targetBuffer, final ByteBuffer targetByteBuffer, final Response<R> response )
    {
        return new Runnable()
        {
            @SuppressWarnings( "unchecked" )
            public void run()
            {
                try
                {
                    type.getObjectSerializer().write( response.response(), targetBuffer );
                    writeStoreId( response.getStoreId(), targetBuffer );
                    writeTransactionStreams( response.transactions(), targetBuffer, targetByteBuffer );
                    targetBuffer.done();
                    responseWritten( type, channel, context );
                }
                catch ( IOException e )
                {
                    e.printStackTrace();
                    throw new RuntimeException( e );
                }
                catch ( RuntimeException e )
                {
                    e.printStackTrace();
                    throw e;
                }
            }
        };
    }
    
    protected abstract void responseWritten( RequestType<M> type, Channel channel, SlaveContext context );

    private static void writeStoreId( StoreId storeId, ChannelBuffer targetBuffer )
    {
        targetBuffer.writeBytes( storeId.serialize() );
    }
    
    private static <T> void writeTransactionStreams( TransactionStream txStream,
            ChannelBuffer buffer, ByteBuffer readBuffer ) throws IOException
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
            datasourceId.put( datasource, i + 1/*0 means "no more transactions"*/);
        }
        for ( Triplet<String, Long, TxExtractor> tx : IteratorUtil.asIterable( txStream ) )
        {
            buffer.writeByte( datasourceId.get( tx.first() ) );
            buffer.writeLong( tx.second() );
            BlockLogBuffer blockBuffer = new BlockLogBuffer( buffer );
            tx.third().extract( blockBuffer );
            blockBuffer.done();
        }
        buffer.writeByte( 0/*no more transactions*/);
    }

    protected SlaveContext readContext( ChannelBuffer buffer )
    {
        int machineId = buffer.readInt();
        int eventIdentifier = buffer.readInt();
        int txsSize = buffer.readByte();
        @SuppressWarnings( "unchecked" )
        Pair<String, Long>[] lastAppliedTransactions = new Pair[txsSize];
        for ( int i = 0; i < txsSize; i++ )
        {
            lastAppliedTransactions[i] = Pair.of( readString( buffer ), buffer.readLong() );
        }
        return new SlaveContext( machineId, eventIdentifier, lastAppliedTransactions );
    }

    protected abstract RequestType<M> getRequestContext( byte id );

    protected Pair<ChannelBuffer, ByteBuffer> mapSlave( Channel channel, SlaveContext slave )
    {
        channelGroup.add( channel );
        Pair<ChannelBuffer, ByteBuffer> buffer = null;
        synchronized ( connectedSlaveChannels )
        {
            if ( slave != null )
            {
                connectedSlaveChannels.put( channel, slave );
            }
            buffer = channelBuffers.get( channel );
            if ( buffer == null )
            {
                buffer = Pair.of( ChannelBuffers.dynamicBuffer(), ByteBuffer.allocateDirect( 1*1024*1024 ) );
                channelBuffers.put( channel, buffer );
            }
            buffer.first().clear();
        }
        return buffer;
    }

    protected void unmapSlave( Channel channel, SlaveContext slave )
    {
        synchronized ( connectedSlaveChannels )
        {
            connectedSlaveChannels.remove( channel );
        }
    }
    
    protected M getMaster()
    {
        return realMaster;
    }

    public void shutdown()
    {
        // Close all open connections
        deadConnectionsPoller.shutdown();
        msgLog.logMessage( "Master server shutdown, closing all channels", true );
        channelGroup.close().awaitUninterruptibly();
        executor.shutdown();
        // TODO This should work, but blocks with busy wait sometimes
//        channelFactory.releaseExternalResources();
    }

    private void checkForDeadChannels()
    {
        synchronized ( connectedSlaveChannels )
        {
            Collection<Channel> channelsToRemove = new ArrayList<Channel>();
            for ( Map.Entry<Channel, SlaveContext> entry : connectedSlaveChannels.entrySet() )
            {
                if ( !channelIsOpen( entry.getKey() ) )
                {
                    msgLog.logMessage( "Found dead channel " + entry.getKey() + ", " + entry.getValue() );
                    finishOffConnection( entry.getKey(), entry.getValue() );
                    msgLog.logMessage( "Removed " + entry.getKey() + ", " + entry.getValue() );
                    channelsToRemove.add( entry.getKey() );
                }
            }
            for ( Channel channel : channelsToRemove )
            {
                connectedSlaveChannels.remove( channel );
                channelBuffers.remove( channel );
                partialRequests.remove( channel );
            }
        }
    }

    protected abstract void finishOffConnection( Channel channel, SlaveContext context );

    private boolean channelIsOpen( Channel channel )
    {
        /**
         * "open" is defined as the lowest means of connectedness
         * "connected" may be that data is actually sent or something
         */
        return channel.isConnected() && channel.isOpen();
    }
    
    public Map<Channel, SlaveContext> getConnectedSlaveChannels()
    {
        return connectedSlaveChannels;
    }

    // =====================================================================
    // Just some methods which aren't really used when running an HA cluster,
    // but exposed so that other tools can reach that information.
    // =====================================================================

    private class PartialRequest
    {
        final SlaveContext context;
        final Pair<ChannelBuffer, ByteBuffer> buffers;
        final RequestType<M> type;

        public PartialRequest( RequestType<M> type, SlaveContext context, Pair<ChannelBuffer, ByteBuffer> buffers )
        {
            this.type = type;
            this.context = context;
            this.buffers = buffers;
        }

        public void add( ChannelBuffer buffer )
        {
            this.buffers.first().writeBytes( buffer );
        }
    }
}
