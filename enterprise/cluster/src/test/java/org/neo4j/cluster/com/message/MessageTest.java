/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.cluster.com.message;

import org.junit.Test;

import java.net.URI;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

public class MessageTest
{
    @Test
    public void respondingToInternalMessageShouldProduceCorrectMessage()
    {
        // Given
        final Object payload = new Object();
        final MessageType type = mock(MessageType.class);
        Message message = Message.internal( type, payload );

        // When
        Message response = Message.respond( type, message, payload );

        // Then
        assertTrue( response.isInternal() );
        assertEquals( payload, response.getPayload() );
        assertEquals( type, response.getMessageType() );
    }

    @Test
    public void respondingToExternalMessageShouldProperlySetToHeaders()
    {
        // Given
        final Object payload = new Object();
        final MessageType type = mock(MessageType.class);
        URI to = URI.create( "cluster://to" );
        URI from = URI.create( "cluster://from" );
        Message incoming = Message.to( type, to, payload );
        incoming.setHeader( Message.HEADER_FROM, from.toString() );

        // When
        Message response = Message.respond( type, incoming, payload );

        // Then
        assertFalse( response.isInternal() );
        assertEquals( from.toString(), response.getHeader( Message.HEADER_TO ) );
        assertEquals( payload, response.getPayload() );
        assertEquals( type, response.getMessageType() );
    }
}
