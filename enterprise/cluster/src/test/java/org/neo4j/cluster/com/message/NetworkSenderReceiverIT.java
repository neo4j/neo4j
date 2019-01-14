/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.cluster.com.message;

import org.junit.Test;
import org.mockito.ArgumentMatchers;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

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
import org.neo4j.ports.allocation.PortAuthority;

import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NetworkSenderReceiverIT
{
    public enum TestMessage implements MessageType
    {
        helloWorld
    }

    @Test
    public void shouldSendAMessageFromAClientWhichIsReceivedByAServer() throws Exception
    {

        // given
        int port1 = PortAuthority.allocatePort();
        int port2 = PortAuthority.allocatePort();

        CountDownLatch latch = new CountDownLatch( 1 );

        LifeSupport life = new LifeSupport();

        Server server1 = new Server( latch, MapUtil.stringMap(
                ClusterSettings.cluster_server.name(), "localhost:" + port1,
                ClusterSettings.server_id.name(), "1",
                ClusterSettings.initial_hosts.name(), "localhost:" + port1 + ",localhost:" + port2 )
        );

        life.add( server1 );

        Server server2 = new Server( latch, MapUtil.stringMap(
                ClusterSettings.cluster_server.name(), "localhost:" + port2,
                ClusterSettings.server_id.name(), "2",
                ClusterSettings.initial_hosts.name(), "localhost:" + port1 + ",localhost:" + port2 )
        );

        life.add( server2 );

        life.start();

        // when

        server1.process( Message.to( TestMessage.helloWorld, URI.create( "cluster://127.0.0.1:" + port2 ),"Hello World" ) );

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
            when( logProviderMock.getLog( ArgumentMatchers.<Class>any() ) ).thenReturn( logMock );

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

            doAnswer( invocation ->
            {
                senderChannelClosed.set( true );
                return null;
            } ).when( logMock ).warn( anyString() );

            int port = PortAuthority.allocatePort();

            receiver = new NetworkReceiver( mock( NetworkReceiver.Monitor.class ), new NetworkReceiver.Configuration()
            {
                @Override
                public HostnamePort clusterServer()
                {
                    return new HostnamePort( "127.0.0.1:" + port );
                }

                @Override
                public int defaultPort()
                {
                    return -1; // never used
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
                    return port;
                }

                @Override
                public int defaultPort()
                {
                    return -1; // never used
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

            receiver.addMessageProcessor( message ->
            {
                received.set( true );
                sem.release();
                return true;
            } );

            receiver.init();
            receiver.start();

            sem.acquire(); // wait for start from listeningAt() in the NetworkChannelsListener

            sender.process( Message.to( TestMessage.helloWorld, URI.create( "cluster://127.0.0.1:" + port ),
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
                sender.process( Message.to( TestMessage.helloWorld, URI.create( "cluster://127.0.0.1:" + port ),
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

            sender.process( Message.to( TestMessage.helloWorld, URI.create( "cluster://127.0.0.1:" + port ),
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
            final Config conf = Config.defaults( config );
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
                    return -1; // never used
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
                    return -1; // never used
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
                public void start()
                {
                    networkReceiver.addMessageProcessor( message ->
                    {
                        // server receives a message
                        processedMessage.set( true );
                        latch.countDown();
                        return true;
                    } );
                }
            } );
        }

        @Override
        public void init()
        {
        }

        @Override
        public void start()
        {

            life.start();
        }

        @Override
        public void stop()
        {
            life.stop();
        }

        @Override
        public void shutdown()
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
