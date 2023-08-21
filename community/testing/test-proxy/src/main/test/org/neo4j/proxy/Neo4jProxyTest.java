/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.proxy;

import org.apache.commons.io.IOUtils;
import org.junit.jupiter.api.Test;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.Executors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class Neo4jProxyTest
{
    @Test
    void successfullySendAndReceiveOneMessage()
    {
        var proxy = TcpCrusherProxy.builder().build();
        var proxyConfig = proxy.getProxyConfig();
        var echoServer = new EchoServer( proxyConfig.listenAddress().getPort() );
        var echoClient = new EchoClient( proxyConfig.advertisedAddress().getHostName(), proxyConfig.advertisedAddress().getPort() );
        var executor = Executors.newFixedThreadPool( 2 );

        //when
        executor.execute( echoServer );
        executor.execute( echoClient );

        //then
        var awaitTime = Duration.ofSeconds( 1 );
        await().atMost( awaitTime ).untilAsserted( () -> assertThat( echoServer.clientConnected ).isTrue() );
        await().atMost( awaitTime ).untilAsserted( () -> assertThat( echoClient.connectToServer ).isTrue() );
        await().atMost( awaitTime ).untilAsserted( () -> assertThat( echoServer.messageRead ).isTrue() );
        await().atMost( awaitTime ).untilAsserted( () -> assertThat( echoClient.messageReceived ).isTrue() );

        List.of( echoClient, echoServer, proxy ).forEach( IOUtils::closeQuietly );
    }

    @Test
    void shouldNotSendOneMessage() throws InterruptedException
    {
        var proxy = TcpCrusherProxy.builder().build();
        var proxyConfig = proxy.getProxyConfig();
        var echoServer = new EchoServer( proxyConfig.listenAddress().getPort() );
        var echoClient = new EchoClient( proxyConfig.advertisedAddress().getHostName(), proxyConfig.advertisedAddress().getPort() );
        var executor = Executors.newFixedThreadPool( 2 );

        //when
        proxy.stopAcceptingConnections();
        executor.execute( echoServer );
        executor.execute( echoClient );

        //then
        var awaitTime = Duration.ofSeconds( 1 );
        await().atMost( awaitTime ).untilAsserted( () -> assertThat( echoServer.clientConnected ).isFalse() );
        await().atMost( awaitTime ).untilAsserted( () -> assertThat( echoClient.connectToServer ).isFalse() );
        await().atMost( awaitTime ).untilAsserted( () -> assertThat( echoServer.messageRead ).isFalse() );
        await().atMost( awaitTime ).untilAsserted( () -> assertThat( echoClient.messageReceived ).isFalse() );

        List.of( echoClient, echoServer, proxy ).forEach( IOUtils::closeQuietly );
    }

    @Test
    void startStoppingShouldCorrectly() throws InterruptedException
    {
        var proxy = TcpCrusherProxy.builder().build();
        var proxyConfig = proxy.getProxyConfig();
        var echoServer1 = new EchoServer( proxyConfig.listenAddress().getPort() );
        var echoClient1 = new EchoClient( proxyConfig.advertisedAddress().getHostName(), proxyConfig.advertisedAddress().getPort() );
        var executor = Executors.newFixedThreadPool( 2 );

        //when server don't accept any connections
        proxy.stopAcceptingConnections();
        executor.execute( echoServer1 );
        executor.execute( echoClient1 );

        //then
        var awaitTime = Duration.ofSeconds( 1 );
        await().atMost( awaitTime ).untilAsserted( () -> assertThat( echoServer1.clientConnected ).isFalse() );
        await().atMost( awaitTime ).untilAsserted( () -> assertThat( echoClient1.connectToServer ).isFalse() );
        await().atMost( awaitTime ).untilAsserted( () -> assertThat( echoServer1.messageRead ).isFalse() );
        await().atMost( awaitTime ).untilAsserted( () -> assertThat( echoClient1.messageReceived ).isFalse() );
        List.of( echoClient1, echoServer1 ).forEach( IOUtils::closeQuietly );

        //given new server and client
        var echoServer2 = new EchoServer( proxyConfig.listenAddress().getPort() );
        var echoClient2 = new EchoClient( proxyConfig.advertisedAddress().getHostName(), proxyConfig.advertisedAddress().getPort() );

        //when server start accepting connections
        proxy.startAcceptingConnections();
        executor.execute( echoServer2 );
        executor.execute( echoClient2 );

        //then
        await().atMost( awaitTime ).untilAsserted( () -> assertThat( echoServer2.clientConnected ).isTrue() );
        await().atMost( awaitTime ).untilAsserted( () -> assertThat( echoClient2.connectToServer ).isTrue() );
        await().atMost( awaitTime ).untilAsserted( () -> assertThat( echoServer2.messageRead ).isTrue() );
        await().atMost( awaitTime ).untilAsserted( () -> assertThat( echoClient2.messageReceived ).isTrue() );
        List.of( echoClient2, echoServer2 ).forEach( IOUtils::closeQuietly );
    }

    private static class EchoServer implements Runnable, Closeable
    {
        private final int port;
        private volatile ServerSocket serverSocket;
        private volatile Socket clientSocket;
        private volatile boolean clientConnected;
        private volatile boolean messageRead;

        public EchoServer( int port )
        {
            this.port = port;
        }

        @Override
        public void run()
        {
            try
            {
                listenAndAnswer();
            }
            catch ( IOException | InterruptedException ignored )
            {
            }
        }

        private void listenAndAnswer() throws IOException, InterruptedException
        {
            this.serverSocket = new ServerSocket( port );
            this.clientSocket = serverSocket.accept();
            clientConnected = true;
            try ( PrintWriter out =
                          new PrintWriter( clientSocket.getOutputStream(), true );
                  BufferedReader in = new BufferedReader(
                          new InputStreamReader( clientSocket.getInputStream() ) ) )
            {
                var message = in.readLine();

                Optional.ofNullable( message )
                        .ifPresent( m ->
                                    {
                                        messageRead = true;
                                        out.println( m );
                                    } );
            }
        }

        @Override
        public void close() throws IOException
        {
            IOUtils.closeQuietly( serverSocket );
            IOUtils.closeQuietly( clientSocket );
        }
    }

    private static class EchoClient implements Runnable, Closeable
    {
        private final String host;
        private final int port;
        private volatile boolean messageReceived;
        private volatile boolean connectToServer;
        private Socket socket;

        public EchoClient( String host, int port )
        {
            this.host = host;
            this.port = port;
        }

        @Override
        public void run()
        {
            try
            {
                connectAndSendAMessage();
            }
            catch ( IOException ignored )
            {
            }
        }

        private void connectAndSendAMessage() throws IOException
        {
            this.socket = new Socket( host, port );
            connectToServer = true;
            try ( PrintWriter out =
                          new PrintWriter( socket.getOutputStream(), true );
                  BufferedReader in = new BufferedReader(
                          new InputStreamReader( socket.getInputStream() ) ) )
            {
                var messageToSend = "Echo";
                out.println( messageToSend );
                var message = in.readLine();
                Optional.ofNullable( message )
                        .filter( m -> m.equals( messageToSend ) )
                        .ifPresent( m -> messageReceived = true );
            }
        }

        @Override
        public void close() throws IOException
        {
            IOUtils.closeQuietly( socket );
        }
    }
}
