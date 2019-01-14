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
package org.neo4j.cluster.com;

import org.jboss.netty.channel.Channel;
import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;
import org.junit.Test;

import java.net.InetSocketAddress;
import java.net.URI;

import org.neo4j.cluster.com.message.Message;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class NetworkReceiverTest
{
    static final int PORT = 1234;

    @Test
    public void testGetURIWithWildCard()
    {
        NetworkReceiver networkReceiver = new NetworkReceiver( mock( NetworkReceiver.Monitor.class ),
                mock( NetworkReceiver.Configuration.class ), mock( LogProvider.class ) );

        // Wildcard should not be resolved here
        final String wildCard = "0.0.0.0";
        URI uri = networkReceiver.getURI( new InetSocketAddress( wildCard, PORT ) );

        assertTrue( wildCard + " does not match Uri host: " + uri.getHost(), wildCard.equals( uri.getHost() ) );
        assertTrue( PORT == uri.getPort() );
    }

    @Test
    public void testGetURIWithLocalHost()
    {
        NetworkReceiver networkReceiver = new NetworkReceiver( mock( NetworkReceiver.Monitor.class ),
                mock( NetworkReceiver.Configuration.class ), mock( LogProvider.class ) );

        // We should NOT do a reverse DNS lookup for hostname. It might not be routed properly.
        URI uri = networkReceiver.getURI( new InetSocketAddress( "localhost", PORT ) );

        assertTrue( "Uri host is not localhost ip: " + uri.getHost(), "127.0.0.1".equals( uri.getHost() ) );
        assertTrue( PORT == uri.getPort() );
    }

    @Test
    public void testMessageReceivedOriginFix() throws Exception
    {
        LogProvider logProvider = mock( LogProvider.class );
        Log log = mock( Log.class );
        when( logProvider.getLog( NetworkReceiver.class ) ).thenReturn( log );
        NetworkReceiver networkReceiver = new NetworkReceiver( mock( NetworkReceiver.Monitor.class ),
                mock( NetworkReceiver.Configuration.class ), logProvider );

        // This defines where message is coming from
        final InetSocketAddress inetSocketAddress = new InetSocketAddress( "localhost", PORT );

        final Channel channel = mock( Channel.class );
        when( channel.getLocalAddress() ).thenReturn( inetSocketAddress );
        when( channel.getRemoteAddress() ).thenReturn( inetSocketAddress );

        ChannelHandlerContext ctx = mock( ChannelHandlerContext.class );
        when( ctx.getChannel() ).thenReturn( channel );

        final Message message = Message.to( () -> "test", new URI( "cluster://anywhere" ) );

        MessageEvent messageEvent = mock( MessageEvent.class );
        when( messageEvent.getRemoteAddress() ).thenReturn( inetSocketAddress );
        when( messageEvent.getMessage() ).thenReturn( message );
        when( messageEvent.getChannel() ).thenReturn( channel );

        // the original HEADER_FROM header should be ignored
        message.setHeader( Message.HEADER_FROM, "cluster://someplace:1234" );

        networkReceiver.new MessageReceiver().messageReceived( ctx, messageEvent );

        assertEquals(
                "HEADER_FROM header should have been changed to visible ip address: " + message.getHeader( Message.HEADER_FROM ),
                "cluster://127.0.0.1:1234", message.getHeader( Message.HEADER_FROM ) );
    }
}
