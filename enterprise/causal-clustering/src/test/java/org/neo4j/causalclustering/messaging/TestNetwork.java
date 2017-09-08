/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.messaging;

import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiFunction;

public class TestNetwork<T>
{
    private final Map<T, Inbound> inboundChannels = new HashMap<>();
    private final Map<T, Outbound> outboundChannels = new HashMap<>();

    private final AtomicLong seqGen = new AtomicLong();
    private final BiFunction<T/*from*/, T/*to*/, Long> latencySpecMillis;

    public TestNetwork( BiFunction<T, T, Long> latencySpecMillis )
    {
        this.latencySpecMillis = latencySpecMillis;
    }

    public void disconnect( T endpoint )
    {
        disconnectOutbound( endpoint );
        disconnectInbound( endpoint );
    }

    public void reconnect( T endpoint )
    {
        reconnectInbound( endpoint );
        reconnectOutbound( endpoint );
    }

    public void reset()
    {
        inboundChannels.values().forEach( Inbound::reconnect );
        outboundChannels.values().forEach( Outbound::reconnect );
    }

    public void disconnectInbound( T endpoint )
    {
        inboundChannels.get( endpoint ).disconnect();
    }

    public void reconnectInbound( T endpoint )
    {
        inboundChannels.get( endpoint ).reconnect();
    }

    public void disconnectOutbound( T endpoint )
    {
        outboundChannels.get( endpoint ).disconnect();
    }

    public void reconnectOutbound( T endpoint )
    {
        outboundChannels.get( endpoint ).reconnect();
    }

    public void start()
    {
        for ( Inbound inbound : inboundChannels.values() )
        {
            inbound.start();
        }

        for ( Outbound outbound : outboundChannels.values() )
        {
            outbound.start();
        }
    }

    public void stop()
    {
        for ( Outbound outbound : outboundChannels.values() )
        {
            try
            {
                outbound.stop();
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
            }
        }

        for ( Inbound inbound : inboundChannels.values() )
        {
            try
            {
                inbound.stop();
            }
            catch ( InterruptedException e )
            {
                Thread.currentThread().interrupt();
            }
        }
    }

    public class Outbound implements org.neo4j.causalclustering.messaging.Outbound<T, Message>
    {
        private NetworkThread networkThread;
        private volatile boolean disconnected = false;
        private T me;

        public Outbound( T me )
        {
            this.me = me;
            outboundChannels.put( me, this );
        }

        public void start()
        {
            networkThread = new NetworkThread();
            networkThread.start();
        }

        public void stop() throws InterruptedException
        {
            networkThread.kill();
        }

        @Override
        public void send( T destination, Message message, boolean block )
        {
            if ( block )
            {
                throw new UnsupportedOperationException( "Not implemented" );
            }
            doSend( destination, message, System.currentTimeMillis() );
        }

        private void doSend( T destination, Message message, long now )
        {
            long atMillis = now + latencySpecMillis.apply( me, destination );
            networkThread.scheduleDelivery( destination, message, atMillis );
        }

        public void disconnect()
        {
            disconnected = true;
        }

        public void reconnect()
        {
            disconnected = false;
        }

        class NetworkThread extends Thread
        {
            private volatile boolean done = false;

            private final TreeSet<MessageContext> msgQueue = new TreeSet<>( (Comparator<MessageContext>) ( o1, o2 ) ->
            {
                int res = Long.compare( o1.atMillis, o2.atMillis );

                if ( res == 0 && o1 != o2 )
                {
                    res = o1.seqNum < o2.seqNum ? -1 : 1;
                }
                return res;
            } );

            public void kill() throws InterruptedException
            {
                done = true;
                this.interrupt();
                this.join();
            }

            private class MessageContext
            {
                private final T destination;
                private final Message message;
                private long atMillis;
                private long seqNum;

                private MessageContext( T destination, Message message, long atMillis )
                {
                    this.destination = destination;
                    this.message = message;
                    this.atMillis = atMillis;
                    this.seqNum = seqGen.getAndIncrement();
                }

                @Override
                public boolean equals( Object o )
                {
                    if ( this == o )
                    {
                        return true;
                    }
                    if ( o == null || getClass() != o.getClass() )
                    {
                        return false;
                    }
                    MessageContext that = (MessageContext) o;
                    return seqNum == that.seqNum;
                }

                @Override
                public int hashCode()
                {
                    return Objects.hash( seqNum );
                }
            }

            public synchronized void scheduleDelivery( T destination, Message message, long atMillis )
            {
                if ( !disconnected )
                {
                    msgQueue.add( new MessageContext( destination, message, atMillis ) );
                    notifyAll();
                }
            }

            @Override
            public synchronized void run()
            {
                while ( !done )
                {
                    long now = System.currentTimeMillis();

                    /* Process message ready for delivery */
                    Iterator<MessageContext> itr = msgQueue.iterator();
                    MessageContext context;
                    while ( itr.hasNext() && (context = itr.next()).atMillis <= now )
                    {
                        itr.remove();
                        Inbound inbound = inboundChannels.get( context.destination );
                        if ( inbound != null )
                        {
                            inbound.deliver( context.message );
                        }
                    }

                    /* Waiting logic */
                    try
                    {
                        try
                        {
                            MessageContext first = msgQueue.first();
                            long waitTime = first.atMillis - System.currentTimeMillis();
                            if ( waitTime > 0 )
                            {
                                wait( waitTime );
                            }
                        }
                        catch ( NoSuchElementException e )
                        {
                            wait( 1000 );
                        }
                    }
                    catch ( InterruptedException e )
                    {
                        done = true;
                    }
                }
            }
        }
    }

    public class Inbound implements org.neo4j.causalclustering.messaging.Inbound<Message>
    {
        private MessageHandler<Message> handler;
        private final BlockingQueue<Message> Q = new ArrayBlockingQueue<>( 64, true );
        private NetworkThread networkThread;

        public Inbound( T endpoint )
        {
            inboundChannels.put( endpoint, this );
        }

        public void start()
        {
            networkThread = new NetworkThread();
            networkThread.start();
        }

        public void stop() throws InterruptedException
        {
            networkThread.kill();
        }

        private volatile boolean disconnected = false;

        public synchronized void deliver( Message message )
        {
            if ( !disconnected )
            {
                // do not throw is the queue is full, emulate the drop of messages instead
                Q.offer( message );
            }
        }

        @Override
        public void registerHandler( MessageHandler<Message> handler )
        {
            this.handler = handler;
        }

        public void disconnect()
        {
            disconnected = true;
        }

        public void reconnect()
        {
            disconnected = false;
        }

        class NetworkThread extends Thread
        {
            private volatile boolean done = false;

            public void kill() throws InterruptedException
            {
                done = true;
                this.interrupt();
                this.join();
            }

            @Override
            public void run()
            {
                while ( !done )
                {
                    try
                    {
                        Message message = Q.poll( 1, TimeUnit.SECONDS );
                        if ( message != null && handler != null )
                        {
                            handler.handle( message );
                        }
                    }
                    catch ( InterruptedException e )
                    {
                        done = true;
                    }
                }
            }
        }
    }
}
