package org.neo4j.kernel.ha;

import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

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
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.ha.Master;
import org.neo4j.kernel.impl.ha.SlaveContext;

/**
 * Sits on the master side, receiving serialized requests from slaves (via
 * {@link MasterClient}). Delegates actual work to {@link MasterImpl}.
 */
public class MasterServer extends CommunicationProtocol implements ChannelPipelineFactory
{
    private final static int DEAD_CONNECTIONS_CHECK_INTERVAL = 10;
    
    private final ChannelFactory channelFactory;
    private final ServerBootstrap bootstrap;
    private final Master realMaster;
    private final ChannelGroup channelGroup;
    private final ScheduledExecutorService deadConnectionsPoller;
    private final Map<Integer, Collection<Channel>> channelsPerClient =
            new HashMap<Integer, Collection<Channel>>();

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
        deadConnectionsPoller = new ScheduledThreadPoolExecutor( 1 );
        deadConnectionsPoller.scheduleWithFixedDelay( new Runnable()
        {
            public void run()
            {
                checkForDeadConnections();
            }
        }, DEAD_CONNECTIONS_CHECK_INTERVAL, DEAD_CONNECTIONS_CHECK_INTERVAL, TimeUnit.SECONDS );
    }

    public ChannelPipeline getPipeline() throws Exception
    {
        ChannelPipeline pipeline = Channels.pipeline();
        pipeline.addLast( "frameDecoder", new LengthFieldBasedFrameDecoder( MAX_FRAME_LENGTH,
                0, 4, 0, 4 ) );
        pipeline.addLast( "frameEncoder", new LengthFieldPrepender( 4 ) );
        pipeline.addLast( "serverHandler", new ServerHandler() );
        return pipeline;
    }

    private class ServerHandler extends SimpleChannelHandler
    {
        @Override
        public void messageReceived( ChannelHandlerContext ctx, MessageEvent e ) throws Exception
        {
            try
            {
                ChannelBuffer message = (ChannelBuffer) e.getMessage();
                Pair<ChannelBuffer, SlaveContext> result = handleRequest( realMaster, message );
                rememberChannel( e.getChannel(), result.other() );
                e.getChannel().write( result.first() );
            }
            catch ( Exception e1 )
            {
                e1.printStackTrace();
                throw e1;
            }
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
        deadConnectionsPoller.shutdown();
        channelGroup.close().awaitUninterruptibly();
        channelFactory.releaseExternalResources();
    }

    private void rememberChannel( Channel channel, SlaveContext other )
    {
        channelGroup.add( channel );
        if ( other == null )
        {
            return;
        }
        
        int id = other.machineId();
        synchronized ( channelsPerClient )
        {
            Collection<Channel> channels = channelsPerClient.get( id );
            if ( channels == null )
            {
                channels = new HashSet<Channel>();
                channelsPerClient.put( id, channels );
                System.out.println( "new client connected " + id + ", " + channel );
            }
            channels.add( channel );
        }
    }

    private void checkForDeadConnections()
    {
        synchronized ( channelsPerClient )
        {
            Collection<Integer> clientsToRemove = new ArrayList<Integer>();
            for ( Map.Entry<Integer, Collection<Channel>> entry : channelsPerClient.entrySet() )
            {
                if ( !entry.getValue().isEmpty() && !pruneDeadChannels( entry.getValue() ) )
                {
                    System.out.println( "Rolling back dead transaction from client " +
                            entry.getKey() + " since it went down" );
                    realMaster.rollbackOngoingTransactions( new SlaveContext( entry.getKey(),
                            new Pair[0] ) );
                    clientsToRemove.add( entry.getKey() );
                }
            }
            
            for ( Integer id : clientsToRemove )
            {
                channelsPerClient.remove( id );
            }
        }
    }

    private boolean pruneDeadChannels( Collection<Channel> channels )
    {
        boolean anyoneAlive = false;
        for ( Channel channel : channels.toArray( new Channel[channels.size()] ) )
        {
            if ( channel.isConnected() )
            {
                anyoneAlive = true;
            }
            else
            {
                channels.remove( channel );
            }
        }
        return anyoneAlive;
    }
    
    // =====================================================================
    // Just some methods which aren't really used when running a HA cluster,
    // but exposed so that other tools can reach that information.
    // =====================================================================
    
    /**
     * Returns pairs of:
     * key: machine ID
     * value: collection of ongoing transaction event identifiers
     */
    public Iterable<Pair<Integer, Collection<Integer>>> getConnectedClients()
    {
        Collection<Integer> clients = null;
        synchronized ( channelsPerClient )
        {
            clients = new ArrayList<Integer>( channelsPerClient.keySet() );
        }
        
        Map<Integer, Collection<Integer>> txs = ((MasterImpl) realMaster).getOngoingTransactions();
        Collection<Pair<Integer, Collection<Integer>>> result =
                new ArrayList<Pair<Integer,Collection<Integer>>>();
        for ( Integer id : clients )
        {
            result.add( new Pair<Integer, Collection<Integer>>( id, txs.get( id ) ) );
        }
        return result;
    }
}
