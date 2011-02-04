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
package org.neo4j.kernel.ha;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
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
import org.neo4j.kernel.impl.util.StringLogger;

/**
 * Sits on the master side, receiving serialized requests from slaves (via
 * {@link MasterClient}). Delegates actual work to {@link MasterImpl}.
 */
public class MasterServer extends CommunicationProtocol implements ChannelPipelineFactory
{
    private final static int DEAD_CONNECTIONS_CHECK_INTERVAL = 3;
    private final static int MAX_NUMBER_OF_CONCURRENT_TRANSACTIONS = 200;

    private final ChannelFactory channelFactory;
    private final ServerBootstrap bootstrap;
    private final Master realMaster;
    private final ChannelGroup channelGroup;
    private final ScheduledExecutorService deadConnectionsPoller;
    private final Map<Channel, SlaveContext> connectedSlaveChannels =
            new HashMap<Channel, SlaveContext>();
    private final Map<Channel, Pair<ChannelBuffer, ByteBuffer>> channelBuffers =
            new HashMap<Channel, Pair<ChannelBuffer,ByteBuffer>>();
    private final ExecutorService executor;
    private final StringLogger msgLog;
    private final Map<Channel, PartialRequest> partialRequests =
            Collections.synchronizedMap( new HashMap<Channel, PartialRequest>() );

    public MasterServer( Master realMaster, final int port, String storeDir )
    {
        this.realMaster = realMaster;
        this.msgLog = StringLogger.getLogger( storeDir + "/messages.log" );
        executor = Executors.newCachedThreadPool();
        channelFactory = new NioServerSocketChannelFactory(
                executor, executor, MAX_NUMBER_OF_CONCURRENT_TRANSACTIONS );
        bootstrap = new ServerBootstrap( channelFactory );
        bootstrap.setPipelineFactory( this );
        /*
        executor.execute( new Runnable()
        {
            public void run()
            {
                Channel channel;
                try
                {
                    channel = bootstrap.bind( new InetSocketAddress( port ) );
                }
                catch ( ChannelException e )
                {
                    msgLog.logMessage( "Failed to bind master server to port " + port, e );
                    return;
                }
                // Add the "server" channel
                channelGroup.add( channel );
                msgLog.logMessage( "Master server bound to " + port, true );
            }
        } );
        //*/
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
        msgLog.logMessage( "Master server bound to " + port, true );

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
                handleRequest( realMaster, message, event.getChannel() );
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
    private void handleRequest( Master realMaster, ChannelBuffer buffer,
            final Channel channel ) throws IOException
    {
        // TODO Too long method, refactor please
        byte continuation = buffer.readByte();
        if ( continuation == ChunkingChannelBuffer.CONTINUATION_MORE )
        {
            PartialRequest partialRequest = partialRequests.get( channel );
            if ( partialRequest == null )
            {
                // This is the first chunk
                RequestType type = RequestType.values()[buffer.readByte()];
                SlaveContext context = null;
                if ( type.includesSlaveContext() )
                {
                    context = readSlaveContext( buffer );
                }
                Pair<ChannelBuffer, ByteBuffer> targetBuffers = mapSlave( channel, context );
                partialRequest = new PartialRequest( type, context, targetBuffers );
                partialRequests.put( channel, partialRequest );
            }
            partialRequest.add( buffer );
        }
        else
        {
            PartialRequest partialRequest = partialRequests.remove( channel );
            RequestType type = null;
            SlaveContext context = null;
            Pair<ChannelBuffer, ByteBuffer> targetBuffers;
            ChannelBuffer bufferToReadFrom = null;
            ChannelBuffer bufferToWriteTo = null;
            if ( partialRequest == null )
            {
                type = RequestType.values()[buffer.readByte()];
                if ( type.includesSlaveContext() )
                {
                    context = readSlaveContext( buffer );
                }
                targetBuffers = mapSlave( channel, context );
                bufferToReadFrom = buffer;
                bufferToWriteTo = targetBuffers.first();
            }
            else
            {
                type = partialRequest.type;
                context = partialRequest.slaveContext;
                targetBuffers = partialRequest.buffers;
                partialRequest.add( buffer );
                bufferToReadFrom = targetBuffers.first();
                bufferToWriteTo = ChannelBuffers.dynamicBuffer();
            }

            bufferToWriteTo.clear();
            final ChunkingChannelBuffer chunkingBuffer = new ChunkingChannelBuffer( bufferToWriteTo, channel, MAX_FRAME_LENGTH );
            final Response<?> response = type.caller.callMaster( realMaster, context,
                    bufferToReadFrom, chunkingBuffer );
            final ByteBuffer targetByteBuffer = targetBuffers.other();
            final RequestType finalType = type;
            final SlaveContext finalContext = context;

            executor.submit( new Runnable()
            {
                public void run()
                {
                    try
                    {
                        finalType.serializer.write( response.response(), chunkingBuffer );
                        if ( finalType.includesSlaveContext() )
                        {
                            writeTransactionStreams( response.transactions(), chunkingBuffer, targetByteBuffer );
                        }
                        chunkingBuffer.done();
                        if ( finalType == RequestType.FINISH || finalType == RequestType.PULL_UPDATES )
                        {
                            unmapSlave( channel, finalContext );
                        }
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
            } );
        }
    }

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
                    System.out.println( "Found dead channel " + entry.getKey() + ", " + entry.getValue() );
                    realMaster.finishTransaction( entry.getValue() );
                    System.out.println( "Removed " + entry.getKey() + ", " + entry.getValue() );
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

    private boolean channelIsOpen( Channel channel )
    {
        /**
         * "open" is defined as the lowest means of connectedness
         * "connected" may be that data is actually sent or something
         */
        return channel.isConnected() && channel.isOpen();
    }

    // =====================================================================
    // Just some methods which aren't really used when running an HA cluster,
    // but exposed so that other tools can reach that information.
    // =====================================================================

    public Map<Integer, Collection<SlaveContext>> getSlaveInformation()
    {
        // Which slaves are connected a.t.m?
        Set<Integer> machineIds = new HashSet<Integer>();
        synchronized ( connectedSlaveChannels )
        {
            for ( SlaveContext context : this.connectedSlaveChannels.values() )
            {
                machineIds.add( context.machineId() );
            }
        }

        // Insert missing slaves into the map so that all connected slave
        // are in the returned map
        Map<Integer, Collection<SlaveContext>> ongoingTransactions =
                ((MasterImpl) realMaster).getOngoingTransactions();
        for ( Integer machineId : machineIds )
        {
            if ( !ongoingTransactions.containsKey( machineId ) )
            {
                ongoingTransactions.put( machineId, Collections.<SlaveContext>emptyList() );
            }
        }
        return new TreeMap<Integer, Collection<SlaveContext>>( ongoingTransactions );
    }

    static class PartialRequest
    {
        final SlaveContext slaveContext;
        final Pair<ChannelBuffer, ByteBuffer> buffers;
        final RequestType type;

        public PartialRequest( RequestType type, SlaveContext slaveContext, Pair<ChannelBuffer, ByteBuffer> buffers )
        {
            this.type = type;
            this.slaveContext = slaveContext;
            this.buffers = buffers;
        }

        public void add( ChannelBuffer buffer )
        {
            this.buffers.first().writeBytes( buffer );
        }
    }
}
