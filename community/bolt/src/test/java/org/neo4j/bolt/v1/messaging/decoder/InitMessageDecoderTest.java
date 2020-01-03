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
package org.neo4j.bolt.v1.messaging.decoder;

import org.junit.jupiter.api.Test;

import java.util.Map;

import org.neo4j.bolt.messaging.Neo4jPack.Unpacker;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.messaging.RequestMessageDecoder;
import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.security.auth.AuthTokenDecoderTest;
import org.neo4j.bolt.v1.messaging.Neo4jPackV1;
import org.neo4j.bolt.v1.messaging.request.InitMessage;
import org.neo4j.bolt.v1.packstream.PackedInputArray;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.neo4j.bolt.v1.messaging.util.MessageMatchers.serialize;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.test.AuthTokenUtil.assertAuthTokenMatches;

class InitMessageDecoderTest extends AuthTokenDecoderTest
{
    private final BoltResponseHandler responseHandler = mock( BoltResponseHandler.class );
    private final RequestMessageDecoder decoder = new InitMessageDecoder( responseHandler );

    @Test
    void shouldReturnCorrectSignature()
    {
        assertEquals( InitMessage.SIGNATURE, decoder.signature() );
    }

    @Test
    void shouldReturnConnectResponseHandler()
    {
        assertEquals( responseHandler, decoder.responseHandler() );
    }

    @Test
    void shouldDecodeAckFailure() throws Exception
    {
        Neo4jPackV1 neo4jPack = new Neo4jPackV1();
        InitMessage originalMessage = new InitMessage( "My Driver", map( "user", "neo4j", "password", "secret" ) );

        PackedInputArray innput = new PackedInputArray( serialize( neo4jPack, originalMessage ) );
        Unpacker unpacker = neo4jPack.newUnpacker( innput );

        // these two steps are executed before decoding in order to select a correct decoder
        unpacker.unpackStructHeader();
        unpacker.unpackStructSignature();

        RequestMessage deserializedMessage = decoder.decode( unpacker );
        assertEquals( originalMessage, deserializedMessage );
    }

    @Override
    protected void testShouldDecodeAuthToken( Map<String,Object> authToken, boolean checkDecodingResult ) throws Exception
    {
        Neo4jPackV1 neo4jPack = new Neo4jPackV1();
        InitMessage originalMessage = new InitMessage( "My Driver", authToken );

        PackedInputArray innput = new PackedInputArray( serialize( neo4jPack, originalMessage ) );
        Unpacker unpacker = neo4jPack.newUnpacker( innput );

        // these two steps are executed before decoding in order to select a correct decoder
        unpacker.unpackStructHeader();
        unpacker.unpackStructSignature();

        RequestMessage deserializedMessage = decoder.decode( unpacker );

        if ( checkDecodingResult )
        {
            assertInitMessageMatches( originalMessage, deserializedMessage );
        }
    }

    private static void assertInitMessageMatches( InitMessage expected, RequestMessage actual )
    {
        assertEquals( expected.userAgent(), ((InitMessage) actual).userAgent() );
        assertAuthTokenMatches( expected.authToken(), ((InitMessage) actual).authToken() );
    }
}
