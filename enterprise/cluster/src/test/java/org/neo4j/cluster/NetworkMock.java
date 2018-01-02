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
package org.neo4j.cluster;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.URI;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

import org.neo4j.cluster.protocol.atomicbroadcast.AtomicBroadcastSerializer;
import org.neo4j.cluster.protocol.atomicbroadcast.ObjectStreamFactory;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.cluster.com.message.MessageType;
import org.neo4j.cluster.protocol.atomicbroadcast.multipaxos.InMemoryAcceptorInstanceStore;
import org.neo4j.cluster.protocol.cluster.ClusterConfiguration;
import org.neo4j.cluster.protocol.election.ServerIdElectionCredentialsProvider;
import org.neo4j.cluster.statemachine.StateTransitionLogger;
import org.neo4j.cluster.timeout.MessageTimeoutStrategy;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.monitoring.Monitors;

/**
 * This mocks message delivery, message loss, and time for timeouts and message latency
 * between protocol servers.
 */
public class NetworkMock
{
    Map<String, TestProtocolServer> participants = new LinkedHashMap<String, TestProtocolServer>();

    private List<MessageDelivery> messageDeliveries = new ArrayList<MessageDelivery>();

    private long now = 0;
    private Monitors monitors;
    private long tickDuration;
    private final MultipleFailureLatencyStrategy strategy;
    private MessageTimeoutStrategy timeoutStrategy;
    private LogService logService;
    protected final Log log;
    private final List<Pair<Future<?>, Runnable>> futureWaiter;


    public NetworkMock( LogService logService, Monitors monitors, long tickDuration, MultipleFailureLatencyStrategy strategy,
                        MessageTimeoutStrategy timeoutStrategy )
    {
        this.monitors = monitors;
        this.tickDuration = tickDuration;
        this.strategy = strategy;
        this.timeoutStrategy = timeoutStrategy;
        this.logService = logService;
        this.log = logService.getInternalLog( NetworkMock.class );
        futureWaiter = new LinkedList<Pair<Future<?>, Runnable>>();
    }

    public TestProtocolServer addServer( int serverId, URI serverUri )
    {
        TestProtocolServer server = newTestProtocolServer( serverId, serverUri );

        debug( serverUri.toString(), "joins network" );

        participants.put( serverUri.toString(), server );

        return server;
    }

    protected TestProtocolServer newTestProtocolServer( int serverId, URI serverUri )
    {
        ProtocolServerFactory protocolServerFactory = new MultiPaxosServerFactory( new ClusterConfiguration( "default", logService.getInternalLogProvider() ), logService.getInternalLogProvider(), monitors.newMonitor( StateMachines.Monitor.class ) );

        ServerIdElectionCredentialsProvider electionCredentialsProvider = new ServerIdElectionCredentialsProvider();
        electionCredentialsProvider.listeningAt( serverUri );
        TestProtocolServer protocolServer = new TestProtocolServer( logService.getInternalLogProvider(), timeoutStrategy, protocolServerFactory, serverUri,
                new InstanceId( serverId ), new InMemoryAcceptorInstanceStore(), electionCredentialsProvider );
        protocolServer.addStateTransitionListener( new StateTransitionLogger( logService.getInternalLogProvider(),
                new AtomicBroadcastSerializer( new ObjectStreamFactory(), new ObjectStreamFactory() ) ) );
        return protocolServer;
    }

    private void debug( String participant, String string )
    {
        log.debug( "=== " + participant + " " + string );
    }

    public void removeServer( String serverId )
    {
        debug( serverId, "leaves network" );
        participants.remove( serverId );
    }

    public void addFutureWaiter( Future<?> future, Runnable toRun )
    {
        futureWaiter.add( Pair.<Future<?>, Runnable>of( future, toRun ) );
    }

    public int tick()
    {
        // Deliver messages whose delivery time has passed
        now += tickDuration;

        //       logger.debug( "tick:"+now );

        Iterator<MessageDelivery> iter = messageDeliveries.iterator();
        while ( iter.hasNext() )
        {
            MessageDelivery messageDelivery = iter.next();
            if ( messageDelivery.getMessageDeliveryTime() <= now )
            {
                long delay = strategy.messageDelay( messageDelivery.getMessage(),
                        messageDelivery.getServer().getServer().boundAt().toString() );
                if ( delay != NetworkLatencyStrategy.LOST )
                {
                    messageDelivery.getServer().process( messageDelivery.getMessage() );
                }
                iter.remove();
            }
        }

        // Check and trigger timeouts
        for ( TestProtocolServer testServer : participants.values() )
        {
            testServer.tick( now );
        }

        // Get all sent messages from all test servers
        List<Message> messages = new ArrayList<Message>();
        for ( TestProtocolServer testServer : participants.values() )
        {
            testServer.sendMessages( messages );
        }

        // Now send them and figure out latency
        for ( Message message : messages )
        {
            String to = message.getHeader( Message.TO );
            long delay = 0;
            if ( message.getHeader( Message.TO ).equals( message.getHeader( Message.FROM ) ) )
            {
                log.debug( "Sending message to itself; zero latency" );
            }
            else
            {
                delay = strategy.messageDelay( message, to );
            }

            if ( delay == NetworkLatencyStrategy.LOST )
            {
                log.debug( "Send message to " + to + " was lost" );
            }
            else
            {
                TestProtocolServer server = participants.get( to );
                log.debug( "Send to " + to + ": " + message );
                messageDeliveries.add( new MessageDelivery( now + delay, message, server ) );
            }
        }

        Iterator<Pair<Future<?>, Runnable>> waiters = futureWaiter.iterator();
        while ( waiters.hasNext() )
        {
            Pair<Future<?>, Runnable> next = waiters.next();
            if ( next.first().isDone() )
            {
                next.other().run();
                waiters.remove();
            }
        }

        return messageDeliveries.size();
    }

    public void tick( int iterations )
    {
        for ( int i = 0; i < iterations; i++ )
        {
            tick();
        }
    }

    public void tickUntilDone()
    {
        while ( tick() + totalCurrentTimeouts() > 0 )
        {
        }
    }

    private int totalCurrentTimeouts()
    {
        int count = 0;
        for ( TestProtocolServer testProtocolServer : participants.values() )
        {
            count += testProtocolServer.getTimeouts().getTimeouts().size();
        }
        return count;
    }

    @Override
    public String toString()
    {
        StringWriter stringWriter = new StringWriter();
        PrintWriter out = new PrintWriter( stringWriter, true );
        out.printf( "Now:%s \n", now );
        out.printf( "Pending messages:%s \n", messageDeliveries.size() );
        out.printf( "Pending timeouts:%s \n", totalCurrentTimeouts() );

        for ( TestProtocolServer testProtocolServer : participants.values() )
        {
            out.println( "  " + testProtocolServer );
        }
        return stringWriter.toString();
    }

    public Long getTime()
    {
        return now;
    }

    public List<TestProtocolServer> getServers()
    {
        return new ArrayList<TestProtocolServer>( participants.values() );
    }

    public MultipleFailureLatencyStrategy getNetworkLatencyStrategy()
    {
        return strategy;
    }

    public MessageTimeoutStrategy getTimeoutStrategy()
    {
        return timeoutStrategy;
    }

    private static class MessageDelivery
    {
        long messageDeliveryTime;
        Message<? extends MessageType> message;
        TestProtocolServer server;

        private MessageDelivery( long messageDeliveryTime, Message<? extends MessageType> message,
                                 TestProtocolServer server )
        {
            this.messageDeliveryTime = messageDeliveryTime;
            this.message = message;
            this.server = server;
        }

        public long getMessageDeliveryTime()
        {
            return messageDeliveryTime;
        }

        public Message<? extends MessageType> getMessage()
        {
            return message;
        }

        public TestProtocolServer getServer()
        {
            return server;
        }

        @Override
        public String toString()
        {
            return "Deliver " + message.getMessageType().name() + " to " + server.getServer().getServerId() + " at "
                    + messageDeliveryTime;
        }
    }
}
