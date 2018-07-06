/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.bolt.v3.messaging;

import org.junit.jupiter.api.Test;

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.messaging.BoltRequestMessageReader;
import org.neo4j.bolt.messaging.Neo4jPack;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.runtime.BoltStateMachine;
import org.neo4j.bolt.v1.messaging.request.InitMessage;
import org.neo4j.bolt.v1.packstream.PackedInputArray;

import static org.hamcrest.CoreMatchers.startsWith;
import static org.junit.Assert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.bolt.v3.messaging.BoltProtocolV3ComponentFactory.encode;
import static org.neo4j.bolt.v3.messaging.BoltProtocolV3ComponentFactory.neo4jPack;
import static org.neo4j.helpers.collection.MapUtil.map;

class BoltRequestMessageReaderV3Test
{
    @Test
    void shouldDecodeHelloMessage() throws Exception
    {
        testMessageDecoding( new HelloMessage( map( "user_agent", "My driver", "one", 1L, "two", 2L ) ) );
    }

    @Test
    void shouldNotDecodeInitMessage() throws Exception
    {
        BoltIOException exception =
                assertThrows( BoltIOException.class, () -> testMessageDecoding( new InitMessage( "My driver", map( "one", 1L, "two", 2L ) ) ) );
        assertThat( exception.getMessage(), startsWith( "Unable to read message type." ) );
    }

    private static void testMessageDecoding( RequestMessage message ) throws Exception
    {
        Neo4jPack neo4jPack = neo4jPack();

        BoltStateMachine stateMachine = mock( BoltStateMachine.class );
        BoltRequestMessageReader reader = BoltProtocolV3ComponentFactory.requestMessageReader( stateMachine );

        PackedInputArray innput = new PackedInputArray( encode( neo4jPack, message ) );
        Neo4jPack.Unpacker unpacker = neo4jPack.newUnpacker( innput );

        reader.read( unpacker );

        verify( stateMachine ).process( eq( message ), any() );
    }
}
