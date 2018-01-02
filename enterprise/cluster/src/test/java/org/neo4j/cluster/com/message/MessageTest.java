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

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import java.net.URI;

import org.junit.Test;

public class MessageTest
{
    @Test
    public void respondingToInternalMessageShouldProduceCorrectMessage() throws Exception
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
    public void respondingToExternalMessageShouldProperlySetToHeaders() throws Exception
    {
        // Given
        final Object payload = new Object();
        final MessageType type = mock(MessageType.class);
        URI to = URI.create( "cluster://to" );
        URI from = URI.create( "cluster://from" );
        Message incoming = Message.to( type, to, payload );
        incoming.setHeader( Message.FROM, from.toString() );

        // When
        Message response = Message.respond( type, incoming, payload );

        // Then
        assertFalse( response.isInternal() );
        assertEquals( from.toString(), response.getHeader( Message.TO ) );
        assertEquals( payload, response.getPayload() );
        assertEquals( type, response.getMessageType() );
    }
}
