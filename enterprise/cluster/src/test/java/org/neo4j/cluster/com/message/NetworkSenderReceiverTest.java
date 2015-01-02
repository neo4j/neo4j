/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.cluster.com.message;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.com.NetworkReceiver;
import org.neo4j.cluster.com.NetworkSender;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.impl.util.StringLogger;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.logging.DevNullLoggingService;
import org.neo4j.kernel.logging.Logging;

import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NetworkSenderReceiverTest
{
    public enum TestMessage implements MessageType
    {
        helloWorld
    }

    @Test
    public void shouldSendAMessageFromAClientWhichIsReceivedByAServer() throws Exception
    {

        // given

        CountDownLatch latch = new CountDownLatch( 1 );

        LifeSupport life = new LifeSupport();

        Server server1 = new Server( latch, MapUtil.stringMap( ClusterSettings.cluster_server.name(),
                "localhost:1234", ClusterSettings.server_id.name(), "1",
                ClusterSettings.initial_hosts.name(), "localhost:1234,localhost:1235" ) );

        life.add( server1 );

        Server server2 = new Server( latch, MapUtil.stringMap( ClusterSettings.cluster_server.name(), "localhost:1235",
                ClusterSettings.server_id.name(), "2",
                ClusterSettings.initial_hosts.name(), "localhost:1234,localhost:1235" ) );

        life.add( server2 );

        life.start();

        // when

        server1.process( Message.to( TestMessage.helloWorld, URI.create( "cluster://127.0.0.1:1235" ), "Hello World" ) );

        // then

        latch.await( 5, TimeUnit.SECONDS );

        assertTrue( "server1 should have processed the message", server1.processedMessage() );
        assertTrue( "server2 should have processed the message", server2.processedMessage() );

        life.shutdown();
    }

    @Test
    public void senderThatStartsAfterReceiverShouldEventuallyConnectSuccessfully() throws Throwable
    {
        /*
         * This test verifies that a closed channel from a sender to a receiver is removed from the connections
         * mapping in the sender. It starts a sender, connects it to a receiver and sends a message
         */
        NetworkSender sender = null;
        NetworkReceiver receiver = null;
        try
        {
            Logging loggingMock = mock( Logging.class );
            StringLogger loggerMock = mock( StringLogger.class );
            when( loggingMock.getMessagesLog( Matchers.<Class>any() ) ).thenReturn( loggerMock );

            final Semaphore sem = new Semaphore( 0 );

            doAnswer( new Answer<Object>()
            {
                @Override
                public Object answer( InvocationOnMock invocation ) throws Throwable
                {
                    sem.release();
                    return null;
                }
            } ).when( loggerMock ).warn( anyString() );

            receiver = new NetworkReceiver( new NetworkReceiver.Configuration()
            {
                @Override
                public HostnamePort clusterServer()
                {
                    return new HostnamePort( "127.0.0.1:1235" );
                }

                @Override
                public int defaultPort()
                {
                    return 5001;
                }

                @Override
                public String name()
                {
                    return null;
                }
            }, new DevNullLoggingService() )
            {
                @Override
                public void stop() throws Throwable
                {
                    super.stop();
                    sem.release();
                }
            };

            sender = new NetworkSender( new NetworkSender.Configuration()
            {
                @Override
                public int port()
                {
                    return 1235;
                }

                @Override
                public int defaultPort()
                {
                    return 5001;
                }
            }, receiver, loggingMock );

            sender.init();
            sender.start();

            receiver.addNetworkChannelsListener( new NetworkReceiver.NetworkChannelsListener()
            {
                @Override
                public void listeningAt( URI me )
                {
                    sem.release();
                }

                @Override
                public void channelOpened( URI to )
                {
                }

                @Override
                public void channelClosed( URI to )
                {
                }
            } );

            final AtomicBoolean received = new AtomicBoolean( false );

            receiver.addMessageProcessor( new MessageProcessor()
            {
                @Override
                public boolean process( Message<? extends MessageType> message )
                {
                    received.set( true );
                    sem.release();
                    return true;
                }
            } );

            receiver.init();
            receiver.start();

            sem.acquire(); // wait for start from listeningAt() in the NetworkChannelsListener

            sender.process( Message.to( TestMessage.helloWorld, URI.create( "cluster://127.0.0.1:1235" ), "Hello World" ) );

            sem.acquire(); // wait for the listeningAt trigger on receive (same as the previous but with real URI this time)
            sem.acquire(); // wait for process from the MessageProcessor

            receiver.stop();

            sem.acquire(); // wait for overridden stop method in receiver

            sender.process( Message.to( TestMessage.helloWorld, URI.create( "cluster://127.0.0.1:1235" ), "Hello World2" ) );

            sem.acquire(); // wait for the warn from the sender

            receiver.start();

            sem.acquire(); // wait for receiver.listeningAt()

            received.set( false );

            sender.process( Message.to( TestMessage.helloWorld, URI.create( "cluster://127.0.0.1:1235" ), "Hello World3" ) );

            sem.acquire(); // wait for receiver.process();

            assertTrue( received.get() );
        }
        finally
        {
            if ( sender != null )
            {
                sender.stop();
                sender.shutdown();
            }
            if ( receiver != null )
            {
                receiver.stop();
                receiver.shutdown();
            }
        }
    }

    private static class Server
            implements Lifecycle, MessageProcessor
    {
        private final NetworkReceiver networkReceiver;
        private final NetworkSender networkSender;

        private final LifeSupport life = new LifeSupport();
        private AtomicBoolean processedMessage = new AtomicBoolean();

        private Server( final CountDownLatch latch, final Map<String, String> config )
        {
            final Config conf = new Config( config, ClusterSettings.class );
            networkReceiver = life.add(new NetworkReceiver(new NetworkReceiver.Configuration()
            {
                @Override
                public HostnamePort clusterServer()
                {
                    return conf.get( ClusterSettings.cluster_server );
                }

                @Override
                public int defaultPort()
                {
                    return 5001;
                }

                @Override
                public String name()
                {
                    return null;
                }
            }, new DevNullLoggingService()));

            networkSender = life.add(new NetworkSender(new NetworkSender.Configuration()
            {
                @Override
                public int defaultPort()
                {
                    return 5001;
                }

                @Override
                public int port()
                {
                    return conf.get( ClusterSettings.cluster_server ).getPort();
                }
            }, networkReceiver, new DevNullLoggingService()));

            life.add( new LifecycleAdapter()
            {
                @Override
                public void start() throws Throwable
                {
                    networkReceiver.addMessageProcessor( new MessageProcessor()
                    {
                        @Override
                        public boolean process( Message<? extends MessageType> message )
                        {
                            // server receives a message
                            processedMessage.set(true);
                            latch.countDown();
                            return true;
                        }
                    } );
                }
            } );
        }

        @Override
        public void init() throws Throwable
        {
        }

        @Override
        public void start() throws Throwable
        {

            life.start();
        }

        @Override
        public void stop() throws Throwable
        {
            life.stop();
        }

        @Override
        public void shutdown() throws Throwable
        {
        }

        @Override
        public boolean process( Message<? extends MessageType> message )
        {
            // server sends a message
            this.processedMessage.set(true);
            return networkSender.process( message );
        }

        public boolean processedMessage()
        {
            return this.processedMessage.get();
        }
    }
}
