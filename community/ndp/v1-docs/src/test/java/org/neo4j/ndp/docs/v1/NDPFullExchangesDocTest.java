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
package org.neo4j.ndp.docs.v1;

import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.ArrayList;
import java.util.Collection;

import org.neo4j.helpers.HostnamePort;
import org.neo4j.ndp.transport.socket.client.Connection;
import org.neo4j.ndp.transport.socket.client.SocketConnection;
import org.neo4j.ndp.transport.socket.client.WebSocketConnection;
import org.neo4j.ndp.transport.socket.integration.Neo4jWithSocket;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.kernel.impl.util.HexPrinter.hex;
import static org.neo4j.ndp.docs.v1.DocExchangeExample.exchange_example;
import static org.neo4j.ndp.docs.v1.DocSerialization.packAndChunk;
import static org.neo4j.ndp.docs.v1.DocsRepository.docs;

@RunWith( Parameterized.class )
public class NDPFullExchangesDocTest
{
    @Rule
    public Neo4jWithSocket neo4j = new Neo4jWithSocket();

    @Parameterized.Parameter( 0 )
    public String testName;

    @Parameterized.Parameter( 1 )
    public DocExchangeExample example;

    @Parameterized.Parameter( 2 )
    public Connection client;

    @Parameterized.Parameter( 3 )
    public HostnamePort address;

    @Parameterized.Parameters( name = "{0}" )
    public static Collection<Object[]> documentedFullProtocolExamples()
    {
        Collection<Object[]> mappings = new ArrayList<>();

        // Load the documented mappings
        HostnamePort socketAddress = new HostnamePort( "localhost:7687" );
        HostnamePort wsAddress = new HostnamePort( "localhost:7688" );
        for ( DocExchangeExample ex : docs().read(
                "dev/transport.asciidoc",
                "code[data-lang=\"ndp_exchange\"]",
                exchange_example ) )
        {
            mappings.add( new Object[]{"Socket    - "+ex.name(), ex, new SocketConnection(), socketAddress} );
            mappings.add( new Object[]{"WebSocket - "+ex.name(), ex, new WebSocketConnection(), wsAddress} );
        }

        for ( DocExchangeExample ex : docs().read(
                "dev/examples.asciidoc",
                "code[data-lang=\"ndp_exchange\"]",
                exchange_example ) )
        {
            mappings.add( new Object[]{"Socket    - "+ex.name(), ex, new SocketConnection(), socketAddress} );
            mappings.add( new Object[]{"WebSocket - "+ex.name(), ex, new WebSocketConnection(), wsAddress} );
        }

        return mappings;
    }

    @After
    public void shutdown() throws Exception
    {
        client.close();
    }

    @Test
    public void serverShouldBehaveAsDocumented() throws Throwable
    {
        for ( DocExchangeExample.Event event : example )
        {
            if ( event.from().equalsIgnoreCase( "client" ) )
            {
                // Play out a client action
                switch ( event.type() )
                {
                case CONNECT:
                    client.connect( address );
                    break;
                case DISCONNECT:
                    client.disconnect();
                    break;
                case SEND:
                    // Ensure the documented binary representation matches the human-readable version in the docs
                    if ( event.hasHumanReadableValue() )
                    {
                        assertThat( "'" + event.humanReadableMessage() + "' should serialize to the documented " +
                                    "binary data.",
                                hex( event.payload() ),
                                equalTo( hex( packAndChunk( event.humanReadableMessage(), 64 ) ) ) );
                    }
                    client.send( event.payload() );
                    break;
                default:
                    throw new RuntimeException( "Unknown client event: " + event.type() );
                }
            }
            else if ( event.from().equalsIgnoreCase( "server" ) )
            {
                // Assert that the server does what the docs say
                // Play out a client action
                switch ( event.type() )
                {
                case DISCONNECT:
                    // There's not really a good way to verify that the remote connection is closed, we can read and
                    // time out, or write perhaps, but that's buggy and racy.. not sure how to test this on this
                    // level.
                    break;
                case SEND:
                    if ( event.hasHumanReadableValue() )
                    {
                        assertThat( "'" + event.humanReadableMessage() + "' should serialize to the documented " +
                                    "binary data.",
                                hex( event.payload() ),
                                equalTo( hex( packAndChunk( event.humanReadableMessage(), 512 ) ) ) );
                    }

                    byte[] recieved = client.recv( event.payload().length );

                    assertThat(
                            hex( recieved ),
                            equalTo( hex( event.payload() ) ) );
                    break;
                default:
                    throw new RuntimeException( "Unknown server event: " + event.type() );
                }
            }
        }
    }
}
