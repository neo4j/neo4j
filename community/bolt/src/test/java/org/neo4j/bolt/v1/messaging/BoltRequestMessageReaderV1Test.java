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
package org.neo4j.bolt.v1.messaging;

import org.junit.jupiter.api.Test;

import org.neo4j.bolt.messaging.BoltRequestMessageReader;
import org.neo4j.bolt.messaging.BoltResponseMessageWriter;
import org.neo4j.bolt.messaging.Neo4jPack;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.runtime.BoltStateMachine;
import org.neo4j.bolt.runtime.SynchronousBoltConnection;
import org.neo4j.bolt.v1.messaging.request.AckFailureMessage;
import org.neo4j.bolt.v1.messaging.request.DiscardAllMessage;
import org.neo4j.bolt.v1.messaging.request.InitMessage;
import org.neo4j.bolt.v1.messaging.request.PullAllMessage;
import org.neo4j.bolt.v1.messaging.request.ResetMessage;
import org.neo4j.bolt.v1.messaging.request.RunMessage;
import org.neo4j.bolt.v1.packstream.PackedInputArray;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.values.AnyValue;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.serialize;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.values.storable.Values.stringValue;
import static org.neo4j.values.virtual.VirtualValues.map;

public class BoltRequestMessageReaderV1Test
{
    @Test
    void shouldDecodeInitMessage() throws Exception
    {
        testMessageDecoding( new InitMessage( "My driver", map( "one", 1L, "two", 2L ) ) );
    }

    @Test
    void shouldDecodeAckFailureMessage() throws Exception
    {
        testMessageDecoding( AckFailureMessage.INSTANCE );
    }

    @Test
    void shouldDecodeResetMessage() throws Exception
    {
        testMessageDecoding( ResetMessage.INSTANCE );
    }

    @Test
    void shouldDecodeRunMessage() throws Exception
    {
        testMessageDecoding( new RunMessage( "RETURN $answer", map( new String[]{"answer"}, new AnyValue[]{stringValue( "42" )} ) ) );
    }

    @Test
    void shouldDecodeDiscardAllMessage() throws Exception
    {
        testMessageDecoding( DiscardAllMessage.INSTANCE );
    }

    @Test
    void shouldDecodePullAllMessage() throws Exception
    {
        testMessageDecoding( PullAllMessage.INSTANCE );
    }

    private static void testMessageDecoding( RequestMessage message ) throws Exception
    {
        Neo4jPack neo4jPack = new Neo4jPackV1();

        BoltStateMachine stateMachine = mock( BoltStateMachine.class );
        BoltRequestMessageReader reader = newReader( stateMachine );

        PackedInputArray innput = new PackedInputArray( serialize( neo4jPack, message ) );
        Neo4jPack.Unpacker unpacker = neo4jPack.newUnpacker( innput );

        reader.read( unpacker );

        verify( stateMachine ).process( eq( message ), any() );
    }

    private static BoltRequestMessageReader newReader( BoltStateMachine stateMachine )
    {
        return new BoltRequestMessageReaderV1( new SynchronousBoltConnection( stateMachine ), mock( BoltResponseMessageWriter.class ),
                NullLogService.getInstance() );
    }
}
