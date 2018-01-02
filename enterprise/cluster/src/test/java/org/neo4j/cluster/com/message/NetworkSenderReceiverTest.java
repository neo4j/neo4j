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
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.logging.NullLogProvider;

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

        server1.process( Message.to( TestMessage.helloWorld, URI.create( "cluster://127.0.0.1:1235" ),
                "Hello World" ) );

        // then

        assertTrue( latch.await( 5, TimeUnit.SECONDS ) );

        assertTrue( "server1 should have processed the message", server1.processedMessage() );
        assertTrue( "server2 should have processed the message", server2.processedMessage() );

        life.shutdown();
    }

    @Test
    public void senderThatStartsAfterReceiverShouldEventuallyConnectSuccessfully() throws Throwable
    {
        /*
         * This test verifies that a closed channel from a sender to a receiver is removed from the connections
         * mapping in the sender. It starts a sender, connects it to a receiver and sends a message.
         *
         * We should be testing this without resorting to using a NetworkReceiver. But, as prophets Mick Jagger and
         * Keith Richards mention in their scriptures, you can't always get what you want. In this case,
         * NetworkSender creates on its own the things required to communicate with the outside world, and this
         * means it creates actual sockets. To interact with it then, we need to setup listeners for those sockets
         * and respond properly. Hence, NetworkReceiver. Yes, this means that this test requires to open actual
         * network sockets.
         *
         * Read on for further hacks in place.
         */
        NetworkSender sender = null;
        NetworkReceiver receiver = null;
        try
        {
            LogProvider logProviderMock = mock( LogProvider.class );
            Log logMock = mock( Log.class );
            when( logProviderMock.getLog( Matchers.<Class>any() ) ).thenReturn( logMock );

            final Semaphore sem = new Semaphore( 0 );

            /*
             * A semaphore AND a boolean? Weird, you may think, as the purpose is clearly to step through the
             * connection setup/teardown process. So, let's discuss what happens here more clearly.
             *
             * The sender and receiver are started. Trapped by the semaphore release on listeningAt()
             * The sender sends through the first message, it is received by the receiver. Trapped by the semaphore
             *      release on listeningAt() which is triggered on the first message receive on the receiver
             * The receiver is stopped, trapped by the overridden stop() method of the logging service.
             * The sender sends a message through, which will trigger the ChannelClosedException. This is where it
             *      gets tricky. See, normally, since we waited for the semaphore on NetworkReceiver.stop() and an
             *      happensBefore edge exists and all these good things, it should be certain that the Receiver is
             *      actually stopped and the message would fail to be sent. That would be too easy though. In reality,
             *      netty will not wait for all listening threads to stop before returning, so the receiver is not
             *      guaranteed to not be listening for incoming connections when stop() returns. This happens rarely,
             *      but the result is that the message "HelloWorld2" should fail with an exception (triggering the warn
             *      method on the logger) but it doesn't. So we can't block, but we must retry until we know the
             *      message failed to be sent and the exception happened, which is what this test is all about. We do
             *      that with a boolean that is tested upon continuously with sent messages until the error happens.
             *      Then we proceed with...
             * The receiver is started. Trapped by the listeningAt() callback.
             * The sender sends a message.
             * The receiver receives it, trapped by the dummy processor added to the receiver.
             */
            final AtomicBoolean senderChannelClosed = new AtomicBoolean( false );

            doAnswer( new Answer<Object>()
            {
                @Override
                public Object answer( InvocationOnMock invocation ) throws Throwable
                {
                    senderChannelClosed.set( true );
                    return null;
                }
            } ).when( logMock ).warn( anyString() );

            receiver = new NetworkReceiver( mock( NetworkReceiver.Monitor.class ), new NetworkReceiver.Configuration()
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
            }, NullLogProvider.getInstance() )
            {
                @Override
                public void stop() throws Throwable
                {
                    super.stop();
                    sem.release();
                }
            };

            sender = new NetworkSender( mock( NetworkSender.Monitor.class ), new NetworkSender.Configuration()
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
            }, receiver, logProviderMock );

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

            sender.process( Message.to( TestMessage.helloWorld, URI.create( "cluster://127.0.0.1:1235" ),
                    "Hello World" ) );

            sem.acquire(); // wait for process from the MessageProcessor

            receiver.stop();

            sem.acquire(); // wait for overridden stop method in receiver


            /*
             * This is the infernal loop of doom. We keep sending messages until one fails with a ClosedChannelException
             * which we have no better way to grab other than through the logger.warn() call which will occur.
             *
             * This code will hang if the warn we rely on is removed or if the receiver never stops - in general, if
             * the closed channel exception is not thrown. This is not an ideal failure mode but it's the best we can
             * do, given that NetworkSender is provided with very few things from its environment.
             */
            while ( !senderChannelClosed.get() )
            {
                sender.process( Message.to( TestMessage.helloWorld, URI.create( "cluster://127.0.0.1:1235" ),
                        "Hello World2" ) );
                /*
                 * This sleep is not necessary, it's just nice. If it's ommitted, everything will work, but we'll
                 * spam messages over the network as fast as possible. Even when the race between send and
                 * receiver.stop() does not occur, we will still send 3-4 messages through at full speed. If it
                 * does occur, then we are looking at hundreds. So we just back off a bit and let things work out.
                 */
                Thread.sleep( 5 );
            }

            receiver.start();

            sem.acquire(); // wait for receiver.listeningAt()

            received.set( false );

            sender.process( Message.to( TestMessage.helloWorld, URI.create( "cluster://127.0.0.1:1235" ),
                    "Hello World3" ) );

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
            networkReceiver = life.add( new NetworkReceiver( mock( NetworkReceiver.Monitor.class ),
                    new NetworkReceiver.Configuration()
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
            }, NullLogProvider.getInstance() ) );

            networkSender = life.add( new NetworkSender( mock( NetworkSender.Monitor.class ),
                    new NetworkSender.Configuration()
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
            }, networkReceiver, NullLogProvider.getInstance() ) );

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
                            processedMessage.set( true );
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
            this.processedMessage.set( true );
            return networkSender.process( message );
        }

        public boolean processedMessage()
        {
            return this.processedMessage.get();
        }
    }
}
