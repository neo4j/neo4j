/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.bolt.v41.messaging;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.messaging.BoltRequestMessageReader;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.packstream.PackedInputArray;
import org.neo4j.bolt.runtime.statemachine.BoltStateMachine;
import org.neo4j.bolt.v3.messaging.request.TransactionInitiatingMessage;
import org.neo4j.bolt.v4.messaging.BeginMessage;
import org.neo4j.bolt.v4.messaging.RunMessage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.bolt.v41.BoltProtocolV41ComponentFactory.encode;
import static org.neo4j.bolt.v41.BoltProtocolV41ComponentFactory.newNeo4jPack;
import static org.neo4j.bolt.v41.BoltProtocolV41ComponentFactory.requestMessageReader;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

class BoltRequestMessageReaderV41Test
{
    @ParameterizedTest
    @MethodSource( "boltV41Messages" )
    void shouldDecodeV41Messages( RequestMessage message ) throws Exception
    {
        testMessageDecoding( message );
    }

    @ParameterizedTest
    @MethodSource( "boltV41UnsupportedMessages" )
    void shouldNotDecodeUnsupportedMessages( RequestMessage message ) throws Exception
    {
        assertThrows( Exception.class, () -> testMessageDecoding( message ) );
    }

    @Test
    void shouldDecodeBoltV3RunAndBeginMessageAsBoltV41Message() throws Exception
    {
        org.neo4j.bolt.v3.messaging.request.RunMessage runMessageV3 = new org.neo4j.bolt.v3.messaging.request.RunMessage( "RETURN 1", EMPTY_MAP );
        org.neo4j.bolt.v3.messaging.request.BeginMessage beginMessageV3 = new org.neo4j.bolt.v3.messaging.request.BeginMessage();

        RunMessage runMessageV4 = new RunMessage( "RETURN 1", EMPTY_MAP );
        BeginMessage beginMessageV4 = new BeginMessage();

        verifyBoltV3MessageIsReadAsBoltV4Message( runMessageV3, runMessageV4 );
        verifyBoltV3MessageIsReadAsBoltV4Message( beginMessageV3, beginMessageV4 );
    }

    private static void verifyBoltV3MessageIsReadAsBoltV4Message( TransactionInitiatingMessage messageV3, TransactionInitiatingMessage messageV4 )
            throws Exception
    {
        Neo4jPack neo4jPack = newNeo4jPack();

        BoltStateMachine stateMachine = mock( BoltStateMachine.class );
        BoltRequestMessageReader reader = requestMessageReader( stateMachine );

        PackedInputArray input = new PackedInputArray( encode( neo4jPack, messageV3 ) );
        Neo4jPack.Unpacker unpacker = neo4jPack.newUnpacker( input );

        reader.read( unpacker );

        verify( stateMachine ).process( eq( messageV4 ), any() );
        assertThat( messageV3.meta() ).isEqualTo( messageV4.meta() );
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

    private static Stream<RequestMessage> boltV41Messages() throws BoltIOException
    {
        return BoltV41Messages.supported();
    }

    private static Stream<RequestMessage> boltV41UnsupportedMessages() throws BoltIOException
    {
        return BoltV41Messages.unsupported();
    }
}
