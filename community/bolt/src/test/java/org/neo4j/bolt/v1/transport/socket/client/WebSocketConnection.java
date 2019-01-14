/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.v1.transport.socket.client;

import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.eclipse.jetty.websocket.client.WebSocketClient;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.impl.util.HexPrinter;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

public class WebSocketConnection implements TransportConnection, WebSocketListener
{
    private final Supplier<WebSocketClient> clientSupplier;
    private final Function<HostnamePort,URI> uriGenerator;

    private final byte[] POISON_PILL = "poison".getBytes();

    private WebSocketClient client;
    private RemoteEndpoint server;

    // Incoming data goes on this queue
    private final LinkedBlockingQueue<byte[]> received = new LinkedBlockingQueue<>();

    // Current input data being handled, popped off of 'received' queue
    private byte[] currentReceiveBuffer;

    // Index into the current receive buffer
    private int currentReceiveIndex;

    public WebSocketConnection()
    {
        this( WebSocketClient::new, address -> URI.create( "ws://" + address.getHost() + ":" + address.getPort() ) );
    }

    public WebSocketConnection( Supplier<WebSocketClient> clientSupplier, Function<HostnamePort,URI> uriGenerator )
    {
        this.clientSupplier = clientSupplier;
        this.uriGenerator = uriGenerator;
    }

    WebSocketConnection( WebSocketClient client )
    {
        this( null, null );
        this.client = client;
    }

    @Override
    public TransportConnection connect( HostnamePort address ) throws Exception
    {
        URI target = uriGenerator.apply( address );

        client = clientSupplier.get();
        client.start();

        Session session;
        try
        {
            session = client.connect( this, target ).get( 10, SECONDS );
        }
        catch ( Exception e )
        {
            throw new IOException( "Failed to connect to the server within 10 seconds", e );
        }

        server = session.getRemote();
        return this;
    }

    @Override
    public TransportConnection send( byte[] rawBytes ) throws IOException
    {
        // The WS client *mutates* the buffer we give it, so we need to copy it here to allow the caller to retain
        // ownership
        ByteBuffer wrap = ByteBuffer.wrap( Arrays.copyOf( rawBytes, rawBytes.length ) );
        server.sendBytes( wrap );
        return this;
    }

    @Override
    public byte[] recv( int length ) throws IOException, InterruptedException
    {
        int remaining = length;
        byte[] target = new byte[remaining];
        while ( remaining > 0 )
        {
            waitForReceivedData( length, remaining, target );
            for ( int i = 0; i < Math.min( remaining, currentReceiveBuffer.length - currentReceiveIndex ); i++ )
            {
                target[length - remaining] = currentReceiveBuffer[currentReceiveIndex++];
                remaining--;
            }
        }
        return target;
    }

    private void waitForReceivedData( int length, int remaining, byte[] target )
            throws InterruptedException, IOException
    {
        long start = System.currentTimeMillis();
        while ( currentReceiveBuffer == null || currentReceiveIndex >= currentReceiveBuffer.length )
        {
            currentReceiveIndex = 0;
            currentReceiveBuffer = received.poll( 10, MILLISECONDS );

            if ( (currentReceiveBuffer == null && (client.isStopped() || client.isStopping())) ||
                    currentReceiveBuffer == POISON_PILL )
            {
                // no data received
                throw new IOException( "Connection closed while waiting for data from the server." );
            }
            if ( System.currentTimeMillis() - start > 30_000 )
            {
                throw new IOException( "Waited 30 seconds for " + remaining + " bytes, " +
                                       "" + (length - remaining) + " was received: " +
                                       HexPrinter.hex( ByteBuffer.wrap( target ), 0, length - remaining ) );
            }
        }
    }

    @Override
    public void disconnect() throws IOException
    {
        if ( client != null )
        {
            try
            {
                client.stop();
            }
            catch ( Exception e )
            {
                throw new IOException( e );
            }
        }
    }

    @Override
    public void onWebSocketBinary( byte[] bytes, int i, int i2 )
    {
        received.add( bytes );
    }

    @Override
    public void onWebSocketClose( int i, String s )
    {
        received.add( POISON_PILL );
    }

    @Override
    public void onWebSocketConnect( Session session )
    {
    }

    @Override
    public void onWebSocketError( Throwable throwable )
    {
    }

    @Override
    public void onWebSocketText( String s )
    {
    }
}
