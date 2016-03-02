/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.bolt.v1.docs;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.bolt.v1.messaging.message.Message;
import org.neo4j.bolt.v1.transport.socket.client.Connection;
import org.neo4j.helpers.HostnamePort;
import org.neo4j.kernel.impl.util.HexPrinter;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.message;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.dechunk;
import static org.neo4j.bolt.v1.transport.integration.TransportTestUtil.recvOneMessage;

public abstract class BoltFullDocTest
{
    private Connection client;

    @Test
    public void serverShouldBehaveAsDocumented() throws Throwable
    {
        for ( DocExchangeExample.Event event : example() )
        {
            if ( event.from().equalsIgnoreCase( "client" ) )
            {
                // Play out a client action
                switch ( event.type() )
                {
                case CONNECT:
                    client.connect( address() );
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
                                equalTo( hex( DocSerialization.packAndChunk( event.humanReadableMessage(), 64 ) ) ) );
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
                switch ( event.type() )
                {
                case DISCONNECT:
                    // There's not really a good way to verify that the remote connection is closed, we can read and
                    // time out, or write perhaps, but that's buggy and racy.. not sure how to test this on this
                    // level.
                    client.disconnect();
                    client = createClient();
                    break;
                case SEND:
                    if ( event.hasHumanReadableValue() )
                    {
                        // Ensure the documented binary representation matches the human-readable version in the docs
                        assertThat( "'" + event.humanReadableMessage() + "' should serialize to the documented " +
                                    "binary data.",
                                hex( event.payload() ),
                                equalTo( hex( DocSerialization
                                        .packAndChunk( event.humanReadableMessage(), 1024 * 8 ) ) ) );

                        // Ensure that the server replies as documented
                        Message serverMessage = readOneMessage( client );
                        assertThat(
                                "The message received from the server should match the documented binary " +
                                "representation. " +
                                "Human-readable message is <" + event.humanReadableMessage() +
                                ">, received message was: " + serverMessage,
                                serverMessage,
                                equalTo( message( dechunk( event.payload() ) ) ) );
                    }
                    else
                    {
                        // Raw data assertions - used for documenting the version negotiation, for instance
                        assertThat(
                                "The data recieved from the server should match the documented binary representation.",
                                hex( client.recv( event.payload().length ) ),
                                equalTo( hex( event.payload() ) ) );
                    }

                    break;
                default:
                    throw new RuntimeException( "Unknown server event: " + event.type() );
                }
            }
        }
    }

    private Message readOneMessage( Connection client ) throws Exception
    {
        return trimMessage( recvOneMessage( client ) );
    }

    @Before
    public void setUp()
    {
        client = createClient();
    }

    @After
    public void shutdown() throws Exception
    {
        if ( client != null )
        {
            client.disconnect();
        }
    }

    //Allows extending classes to tamper with the message before comparing it with
    //what is documented
    protected Message trimMessage( Message message )
    {
        return message;
    }

    protected abstract Connection createClient();

    protected abstract HostnamePort address();

    protected abstract DocExchangeExample example();

    private static String hex( byte[] payload )
    {
        return HexPrinter.hex( payload, 4, "  " );
    }
}
