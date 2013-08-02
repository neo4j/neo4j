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
package org.neo4j.cluster.com.message;

import java.net.URI;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.Test;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.cluster.com.NetworkReceiver;
import org.neo4j.cluster.com.NetworkSender;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.helpers.collection.MapUtil;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.logging.DevNullLoggingService;

import static org.junit.Assert.assertTrue;

/**
 * TODO
 */
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

        server1.process( Message.to( TestMessage.helloWorld, URI.create( "neo4j://127.0.0.1:1235" ), "Hello World" ) );

        // then

        latch.await( 5, TimeUnit.SECONDS );

        assertTrue( "server1 should have processed the message", server1.processedMessage() );
        assertTrue( "server2 should have processed the message", server2.processedMessage() );

        life.shutdown();
    }

    private static class Server
            implements Lifecycle, MessageProcessor
    {
        private final NetworkReceiver networkReceiver;
        private final NetworkSender networkSender;

        private final LifeSupport life = new LifeSupport();
        private AtomicBoolean processedMessage = new AtomicBoolean(  );

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
            }, new DevNullLoggingService()));

            networkSender = life.add(new NetworkSender(new NetworkSender.Configuration()
            {
                @Override
                public int defaultPort()
                {
                    return 5001;
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
                            System.out.println("#"+message);
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
