/*
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.ndp.transport.socket.client;

import org.eclipse.jetty.util.ssl.SslContextFactory;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.WebSocketListener;
import org.eclipse.jetty.websocket.client.WebSocketClient;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.impl.util.HexPrinter;

public class SecureWebSocketConnection implements Connection, WebSocketListener
{
    private WebSocketClient client;
    private RemoteEndpoint server;

    // Incoming data goes on this queue
    private final ArrayBlockingQueue<byte[]> received = new ArrayBlockingQueue<>( 4 );

    // Current input data being handled, poppoed off of 'recieved' queue
    private byte[] currentRecieveBuffer = null;

    // Index into the current recieve buffer
    private int currentRecieveIndex = 0;

    @Override
    public Connection connect( HostnamePort address ) throws Exception
    {
        URI target = URI.create( "wss://" + address.getHost() + ":" + address.getPort() );

        client = new WebSocketClient( new SslContextFactory( /* trustall= */ true ) );
        client.start();

        Session session = client.connect( this, target ).get( 30, TimeUnit.SECONDS );
        server = session.getRemote();
        return this;
    }

    @Override
    public Connection send( byte[] rawBytes ) throws Exception
    {
        // The WS client *mutates* the buffer we give it, so we need to copy it here to allow the caller to retain
        // ownership
        ByteBuffer wrap = ByteBuffer.wrap( Arrays.copyOf( rawBytes, rawBytes.length ) );
        server.sendBytes( wrap );
        return this;
    }

    @Override
    public byte[] recv( int length ) throws Exception
    {
        int remaining = length;
        byte[] target = new byte[remaining];
        while(remaining > 0)
        {
            waitForRecievedData( length, remaining, target );
            for ( int i = 0; i < Math.min( remaining, currentRecieveBuffer.length - currentRecieveIndex ); i++ )
            {
                target[ length - remaining ] = currentRecieveBuffer[currentRecieveIndex++];
                remaining--;
            }
        }
        return target;
    }

    @Override
    public void discard( int length ) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    private void waitForRecievedData( int length, int remaining, byte[] target ) throws InterruptedException
    {
        while( currentRecieveBuffer == null || currentRecieveIndex >= currentRecieveBuffer.length )
        {
            currentRecieveIndex = 0;
            currentRecieveBuffer = received.poll(30, TimeUnit.SECONDS);
            if(currentRecieveBuffer == null)
            {
                throw new RuntimeException( "Waited 30 seconds for " + remaining + " bytes, " +
                                            "" + (length - remaining) + " was recieved: " +
                                            HexPrinter.hex( ByteBuffer.wrap( target ), 0, length - remaining ) );
            }
        }
    }

    @Override
    public void disconnect() throws Exception
    {
        close();
    }

    @Override
    public void close() throws Exception
    {
        if(client != null )
        {
            client.stop();
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
