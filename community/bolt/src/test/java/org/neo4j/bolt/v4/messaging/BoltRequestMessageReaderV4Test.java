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
package org.neo4j.bolt.v4.messaging;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.util.stream.Stream;

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.messaging.BoltRequestMessageReader;
import org.neo4j.bolt.messaging.Neo4jPack;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.runtime.BoltStateMachine;
import org.neo4j.bolt.v1.messaging.request.AckFailureMessage;
import org.neo4j.bolt.v1.messaging.request.DiscardAllMessage;
import org.neo4j.bolt.v1.messaging.request.InitMessage;
import org.neo4j.bolt.v1.messaging.request.PullAllMessage;
import org.neo4j.bolt.v1.messaging.request.ResetMessage;
import org.neo4j.bolt.v1.packstream.PackedInputArray;
import org.neo4j.bolt.v3.messaging.request.TransactionInitiatingMessage;

import static java.util.Collections.singletonMap;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.bolt.v3.messaging.request.CommitMessage.COMMIT_MESSAGE;
import static org.neo4j.bolt.v3.messaging.request.RollbackMessage.ROLLBACK_MESSAGE;
import static org.neo4j.bolt.v4.BoltProtocolV4ComponentFactory.encode;
import static org.neo4j.bolt.v4.BoltProtocolV4ComponentFactory.newNeo4jPack;
import static org.neo4j.bolt.v4.BoltProtocolV4ComponentFactory.requestMessageReader;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.kernel.impl.util.ValueUtils.asMapValue;
import static org.neo4j.values.virtual.VirtualValues.EMPTY_MAP;

class BoltRequestMessageReaderV4Test
{
    @ParameterizedTest
    @MethodSource( "boltV4Messages" )
    void shouldDecodeV4Messages( RequestMessage message ) throws Exception
    {
        testMessageDecoding( message );
    }

    @ParameterizedTest
    @MethodSource( "boltV4UnsupportedMessages" )
    void shouldNotDecodeUnsupportedMessages( RequestMessage message ) throws Exception
    {
        assertThrows( Exception.class, () -> testMessageDecoding( message ) );
    }

    @Test
    void shouldDecodeBoltV3RunAndBeginMessageAsBoltV4Message() throws Exception
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
        assertThat( messageV3.meta(), equalTo( messageV4.meta() ) );
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

    private static Stream<RequestMessage> boltV4Messages() throws BoltIOException
    {
        return Stream.of(
                new PullMessage( asMapValue( singletonMap( "n",  100L ) ) ),
                new DiscardMessage( asMapValue( singletonMap( "n", 100L ) ) ),
                new RunMessage( "RETURN 1", EMPTY_MAP ),
                new BeginMessage(),

                COMMIT_MESSAGE,
                ROLLBACK_MESSAGE,
                ResetMessage.INSTANCE );
    }

    private static Stream<RequestMessage> boltV4UnsupportedMessages() throws BoltIOException
    {
        return Stream.of(
                new InitMessage( "My driver", map( "one", 1L, "two", 2L ) ),
                AckFailureMessage.INSTANCE,
                new org.neo4j.bolt.v1.messaging.request.RunMessage( "RETURN 1" ),
                PullAllMessage.INSTANCE,
                DiscardAllMessage.INSTANCE
        );
    }
}
