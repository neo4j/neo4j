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
package org.neo4j.bolt.v41.messaging.decoder;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.neo4j.bolt.messaging.BoltIOException;
import org.neo4j.bolt.messaging.RequestMessage;
import org.neo4j.bolt.messaging.RequestMessageDecoder;
import org.neo4j.bolt.packstream.Neo4jPack;
import org.neo4j.bolt.packstream.PackedInputArray;
import org.neo4j.bolt.runtime.BoltResponseHandler;
import org.neo4j.bolt.security.auth.AuthTokenDecoderTest;
import org.neo4j.bolt.v41.messaging.RoutingContext;
import org.neo4j.bolt.v41.messaging.request.HelloMessage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.neo4j.bolt.v3.BoltProtocolV3ComponentFactory.encode;
import static org.neo4j.bolt.v3.BoltProtocolV3ComponentFactory.newNeo4jPack;
import static org.neo4j.internal.helpers.collection.MapUtil.map;
import static org.neo4j.internal.helpers.collection.MapUtil.stringMap;
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
        HelloMessage originalMessage = new HelloMessage( map( "user_agent", "My Driver", "user", "neo4j", "password", "secret", "routing",
                                                              Collections.emptyMap() ),
                                                         new RoutingContext( true, Collections.emptyMap() ),
                                                         map( "user_agent", "My Driver", "user", "neo4j", "password", "secret" ) );
        assertOriginalMessageEqualsToDecoded( originalMessage, decoder );
    }

    @Test
    void shouldDecodeHelloMessageWithRouting() throws Exception
    {
        HelloMessage originalMessage = new HelloMessage( map( "user_agent", "My Driver", "user", "neo4j",
                                                              "password", "secret", "routing", map( "policy", "europe" ) ),
                                                         new RoutingContext( true, stringMap( "policy", "europe" ) ),
                                                         map( "user_agent", "My Driver", "user", "neo4j", "password", "secret" ) );

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
        assertAuthTokenDoesNotContainRoutingContext( deserializedMessage );
    }

    @Test
    void testShouldDecodeRoutingContext() throws Exception
    {
        Map<String,Object> meta = new HashMap<>();
        Map<String,Object> authToken;
        Map<String,String> parameterMap = new HashMap<>();
        RoutingContext routingContext = new RoutingContext( true, parameterMap );

        parameterMap.put( "policy", "fast" );
        parameterMap.put( "region", "eu-west" );

        Neo4jPack neo4jPack = newNeo4jPack();
        meta.put( "user_agent", "My Driver" );
        meta.put( "routing", parameterMap );

        authToken = new HashMap<>( Map.copyOf( meta ) );
        authToken.remove( "routing" );

        HelloMessage originalMessage = new HelloMessage( meta, routingContext, authToken );

        PackedInputArray input = new PackedInputArray( encode( neo4jPack, originalMessage ) );
        Neo4jPack.Unpacker unpacker = neo4jPack.newUnpacker( input );

        // these two steps are executed before decoding in order to select a correct decoder
        unpacker.unpackStructHeader();
        unpacker.unpackStructSignature();

        RequestMessage deserializedMessage = decoder.decode( unpacker );

        assertHelloMessageMatches( originalMessage, deserializedMessage );
        assertRoutingContextMatches( originalMessage, deserializedMessage );
    }

    @Test
    void testShouldDecodeWhenNoRoutingContextProvided() throws Exception
    {
        Map<String,Object> meta = new HashMap<>();

        Neo4jPack neo4jPack = newNeo4jPack();
        meta.put( "user_agent", "My Driver" );
        HelloMessage originalMessage = new HelloMessage( meta, new RoutingContext( false, Collections.emptyMap() ), meta );

        PackedInputArray input = new PackedInputArray( encode( neo4jPack, originalMessage ) );
        Neo4jPack.Unpacker unpacker = neo4jPack.newUnpacker( input );

        // these two steps are executed before decoding in order to select a correct decoder
        unpacker.unpackStructHeader();
        unpacker.unpackStructSignature();

        RequestMessage deserializedMessage = decoder.decode( unpacker );

        assertHelloMessageMatches( originalMessage, deserializedMessage );
        assertRoutingContextMatches( originalMessage, deserializedMessage );
    }

    @Test
    void testShouldDecodeWhenEmptyRoutingContextProvided() throws Exception
    {
        Map<String,Object> meta = new HashMap<>();
        Map<String,String> parameterMap = new HashMap<>();
        RoutingContext routingContext = new RoutingContext( true, parameterMap );

        Neo4jPack neo4jPack = newNeo4jPack();
        meta.put( "user_agent", "My Driver" );
        meta.put( "routing", parameterMap );
        HelloMessage originalMessage = new HelloMessage( meta, routingContext, meta );

        PackedInputArray input = new PackedInputArray( encode( neo4jPack, originalMessage ) );
        Neo4jPack.Unpacker unpacker = neo4jPack.newUnpacker( input );

        // these two steps are executed before decoding in order to select a correct decoder
        unpacker.unpackStructHeader();
        unpacker.unpackStructSignature();

        RequestMessage deserializedMessage = decoder.decode( unpacker );

        assertHelloMessageMatches( originalMessage, deserializedMessage );
        assertRoutingContextMatches( originalMessage, deserializedMessage );
    }

    @Override
    protected void testShouldDecodeAuthToken( Map<String,Object> authToken ) throws Exception
    {
        Neo4jPack neo4jPack = newNeo4jPack();
        authToken.put( "user_agent", "My Driver" );
        HelloMessage originalMessage = new HelloMessage( authToken, new RoutingContext( true,Collections.emptyMap() ), authToken );

        PackedInputArray input = new PackedInputArray( encode( neo4jPack, originalMessage ) );
        Neo4jPack.Unpacker unpacker = neo4jPack.newUnpacker( input );

        // these two steps are executed before decoding in order to select a correct decoder
        unpacker.unpackStructHeader();
        unpacker.unpackStructSignature();

        RequestMessage deserializedMessage = decoder.decode( unpacker );

        assertHelloMessageMatches( originalMessage, deserializedMessage );
    }

    @Test
    protected void testShouldErrorForMissingUserAgent() throws Exception
    {
        Neo4jPack neo4jPack = newNeo4jPack();
        Map<String,Object> authToken = new HashMap<>();
        org.neo4j.bolt.v3.messaging.request.HelloMessage originalMessage = new org.neo4j.bolt.v3.messaging.request.HelloMessage( authToken );

        PackedInputArray input = new PackedInputArray( encode( neo4jPack, originalMessage ) );
        Neo4jPack.Unpacker unpacker = neo4jPack.newUnpacker( input );

        // these two steps are executed before decoding in order to select a correct decoder
        unpacker.unpackStructHeader();
        unpacker.unpackStructSignature();

        BoltIOException exception = assertThrows( BoltIOException.class, () -> decoder.decode( unpacker ) );
        assertEquals( "Expected \"user_agent\" in metadata", exception.getMessage() );
    }

    @Test
    public void shouldThrowExceptionOnIncorrectRoutingContextFormat() throws Exception
    {
        Map<String,Object> meta = new HashMap<>();
        Map<String,Integer> routingContext = new HashMap<>(); // incorrect Map type params
        routingContext.put( "policy", 4 );

        Neo4jPack neo4jPack = newNeo4jPack();
        meta.put( "user_agent", "My Driver" );
        meta.put( "routing", routingContext );
        HelloMessage originalMessage = new HelloMessage( meta, null, meta );

        PackedInputArray input = new PackedInputArray( encode( neo4jPack, originalMessage ) );
        Neo4jPack.Unpacker unpacker = neo4jPack.newUnpacker( input );

        // these two steps are executed before decoding in order to select a correct decoder
        unpacker.unpackStructHeader();
        unpacker.unpackStructSignature();

        BoltIOException ex = assertThrows( BoltIOException.class, () -> decoder.decode( unpacker ) );

        assertEquals( "Routing information in the HELLO message must be a map with string keys and string values.", ex.getMessage() );
    }

    private static void assertHelloMessageMatches( HelloMessage expected, RequestMessage actual )
    {
        assertAuthTokenMatches( expected.meta(), ((HelloMessage) actual).meta() );
    }

    private static void assertRoutingContextMatches( HelloMessage expected, RequestMessage actual )
    {
        assertEquals( expected.routingContext(), ((HelloMessage) actual).routingContext() );
    }

    private static void assertAuthTokenDoesNotContainRoutingContext( RequestMessage message )
    {
        assertFalse( ((HelloMessage) message).authToken().containsKey( "routing" ) );
    }
}
