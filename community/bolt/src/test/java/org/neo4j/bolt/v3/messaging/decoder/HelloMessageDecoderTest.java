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
package org.neo4j.bolt.v3.messaging.decoder;

import org.junit.jupiter.api.Test;

import java.util.Map;

import org.neo4j.bolt.messaging.Neo4jPack;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.messaging.RequestMessageDecoder;
import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.security.auth.AuthTokenDecoderTest;
import org.neo4j.bolt.v1.packstream.PackedInputArray;
import org.neo4j.bolt.v3.messaging.request.HelloMessage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.mock;
import static org.neo4j.bolt.v3.messaging.BoltProtocolV3ComponentFactory.encode;
import static org.neo4j.bolt.v3.messaging.BoltProtocolV3ComponentFactory.newNeo4jPack;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.test.AuthTokenUtil.assertAuthTokenMatches;

class HelloMessageDecoderTest extends AuthTokenDecoderTest
{
    private final BoltResponseHandler responseHandler = mock( BoltResponseHandler.class );
    private final RequestMessageDecoder decoder = new HelloMessageDecoder( responseHandler );

    @Test
    void shouldReturnCorrectSignature()
    {
        assertEquals( HelloMessage.SIGNATURE, decoder.signature() );
    }

    @Test
    void shouldReturnConnectResponseHandler()
    {
        assertEquals( responseHandler, decoder.responseHandler() );
    }

    @Test
    void shouldDecodeHelloMessage() throws Exception
    {
        HelloMessage originalMessage = new HelloMessage( map( "user_agent", "My Driver", "user", "neo4j", "password", "secret" ) );
        assertOriginalMessageEqualsToDecoded( originalMessage, decoder );
    }

    static void assertOriginalMessageEqualsToDecoded( RequestMessage originalMessage, RequestMessageDecoder decoder ) throws Exception
    {
        Neo4jPack neo4jPack = newNeo4jPack();

        PackedInputArray input = new PackedInputArray( encode( neo4jPack, originalMessage ) );
        Neo4jPack.Unpacker unpacker = neo4jPack.newUnpacker( input );

        // these two steps are executed before decoding in order to select a correct decoder
        unpacker.unpackStructHeader();
        unpacker.unpackStructSignature();

        RequestMessage deserializedMessage = decoder.decode( unpacker );
        assertEquals( originalMessage, deserializedMessage );
    }

    @Override
    protected void testShouldDecodeAuthToken( Map<String,Object> authToken ) throws Exception
    {
        Neo4jPack neo4jPack = newNeo4jPack();
        authToken.put( "user_agent", "My Driver" );
        HelloMessage originalMessage = new HelloMessage( authToken );

        PackedInputArray input = new PackedInputArray( encode( neo4jPack, originalMessage ) );
        Neo4jPack.Unpacker unpacker = neo4jPack.newUnpacker( input );

        // these two steps are executed before decoding in order to select a correct decoder
        unpacker.unpackStructHeader();
        unpacker.unpackStructSignature();

        RequestMessage deserializedMessage = decoder.decode( unpacker );
        assertHelloMessageMatches( originalMessage, deserializedMessage );
    }

    @Override
    protected void testShouldFailToDecodeAuthToken( Map<String,Object> authToken, String expectedErrorMessage ) throws Exception
    {
        Neo4jPack neo4jPack = newNeo4jPack();
        authToken.put( "user_agent", "My Driver" );
        HelloMessage originalMessage = new HelloMessage( authToken );

        PackedInputArray input = new PackedInputArray( encode( neo4jPack, originalMessage ) );
        Neo4jPack.Unpacker unpacker = neo4jPack.newUnpacker( input );

        // these two steps are executed before decoding in order to select a correct decoder
        unpacker.unpackStructHeader();
        unpacker.unpackStructSignature();

        try
        {
            decoder.decode( unpacker );
            fail( "Expected UnsupportedOperationException" );
        }
        catch ( UnsupportedOperationException e )
        {
            // Expected
            assertEquals( e.getMessage(), expectedErrorMessage );
        }
    }

    private static void assertHelloMessageMatches( HelloMessage expected, RequestMessage actual )
    {
        assertAuthTokenMatches( expected.meta(), ((HelloMessage) actual).meta() );
    }
}
