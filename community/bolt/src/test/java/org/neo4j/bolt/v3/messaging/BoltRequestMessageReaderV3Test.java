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
package org.neo4j.bolt.v3.messaging;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.messaging.BoltRequestMessageReader;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.packstream.PackedInputArray;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachine;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.bolt.v3.BoltProtocolV3ComponentFactory.encode;
import static org.neo4j.bolt.v3.BoltProtocolV3ComponentFactory.newNeo4jPack;
import static org.neo4j.bolt.v3.BoltProtocolV3ComponentFactory.requestMessageReader;

class BoltRequestMessageReaderV3Test
{
    @ParameterizedTest
    @MethodSource( "boltV3Messages" )
    void shouldDecodeV3Messages( RequestMessage message ) throws Exception
    {
        testMessageDecoding( message );
    }

    @ParameterizedTest
    @MethodSource( "boltV3UnsupportedMessages" )
    void shouldNotDecodeUnsupportedMessages( RequestMessage message ) throws Exception
    {
        assertThrows( Exception.class, () -> testMessageDecoding( message ) );
    }

    private static void testMessageDecoding( RequestMessage message ) throws Exception
    {
        Neo4jPack neo4jPack = newNeo4jPack();

        BoltStateMachine stateMachine = mock( BoltStateMachine.class );
        BoltRequestMessageReader reader = requestMessageReader( stateMachine );

        PackedInputArray input = new PackedInputArray( encode( neo4jPack, message ) );
        Neo4jPack.Unpacker unpacker = neo4jPack.newUnpacker( input );

        reader.read( unpacker );

        verify( stateMachine ).process( eq( message ), any() );
    }

    private static Stream<RequestMessage> boltV3Messages() throws BoltIOException
    {
        return BoltV3Messages.supported();
    }

    private static Stream<RequestMessage> boltV3UnsupportedMessages() throws BoltIOException
    {
        return BoltV3Messages.unsupported();
    }

}
